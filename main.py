#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SPACEMAN con servidor HTTP y WebSocket
- Conecta al WebSocket de Pragmatic Play
- Almacena eventos en SQLite con reset automático cada 1000 eventos (conserva últimos 100)
- Envía solo los últimos 100 eventos al conectar
- Eventos en lotes de hasta 20 cada 1 segundo
- Tabla de niveles enviada cada 60-120 segundos (aleatorio)
- Persistencia con SQLite (archivo se limpia periódicamente)
- Reconexión automática con backoff exponencial
- Auto‑ping cada 10 minutos
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import logging
import os
import random
from datetime import datetime
from typing import Set, Dict, Any, List
from collections import defaultdict
import aiosqlite

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN SPACEMAN
# ============================================
SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301
DB_PATH = "spaceman_data.db"

BASE_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0

spaceman_last_multiplier: float = None
spaceman_events_seen: Set[str] = set()
spaceman_history: list = []
MAX_HISTORY = 100
RESET_THRESHOLD = 1000          # Número de eventos nuevos que dispara la limpieza
KEEP_AFTER_RESET = 100          # Eventos que se conservan después del reset

current_level = 0
level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})

connected_clients: Set[web.WebSocketResponse] = set()

# Variables para generación de IDs locales
_last_gen_time = 0.0
_gen_counter = 0

# Batching
event_queue = asyncio.Queue()
BATCH_SIZE = 20
BATCH_TIMEOUT = 1.0

TABLE_UPDATE_MIN = 60
TABLE_UPDATE_MAX = 120

# Contador de eventos desde el último reset (en memoria, se reinicia al resetear DB)
event_counter_since_reset = 0

# ============================================
# FUNCIONES DE BASE DE DATOS
# ============================================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                maxMultiplier REAL,
                timestamp_recepcion TEXT,
                nivel INTEGER
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS counts (
                level INTEGER,
                range TEXT,
                count INTEGER,
                PRIMARY KEY (level, range)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await db.commit()

async def load_from_db():
    global spaceman_history, spaceman_events_seen, level_counts, current_level, event_counter_since_reset
    async with aiosqlite.connect(DB_PATH) as db:
        # Cargar historial (últimos MAX_HISTORY eventos)
        async with db.execute('SELECT id, maxMultiplier, timestamp_recepcion, nivel FROM events ORDER BY timestamp_recepcion DESC LIMIT ?', (MAX_HISTORY,)) as cursor:
            rows = await cursor.fetchall()
            spaceman_history = []
            spaceman_events_seen.clear()
            for row in rows:
                event = {
                    'event_id': row[0],
                    'maxMultiplier': row[1],
                    'timestamp_recepcion': row[2],
                    'nivel': row[3]
                }
                spaceman_history.append(event)
                spaceman_events_seen.add(row[0])

        # Cargar conteos por nivel
        async with db.execute('SELECT level, range, count FROM counts') as cursor:
            rows = await cursor.fetchall()
            level_counts.clear()
            for level, rng, cnt in rows:
                level_counts[level][rng] = cnt

        # Cargar nivel actual
        async with db.execute('SELECT value FROM state WHERE key = "current_level"') as cursor:
            row = await cursor.fetchone()
            if row:
                current_level = int(row[0])
            else:
                current_level = 0
                await db.execute('INSERT OR IGNORE INTO state (key, value) VALUES (?, ?)', ('current_level', '0'))
                await db.commit()

        # Determinar cuántos eventos hay actualmente para inicializar el contador de reset
        async with db.execute('SELECT COUNT(*) FROM events') as cursor:
            row = await cursor.fetchone()
            event_counter_since_reset = row[0] if row else 0

async def save_event(event: dict):
    global event_counter_since_reset
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR REPLACE INTO events (id, maxMultiplier, timestamp_recepcion, nivel)
            VALUES (?, ?, ?, ?)
        ''', (event['event_id'], event['maxMultiplier'], event['timestamp_recepcion'], event['nivel']))
        await db.commit()
    event_counter_since_reset += 1

    # Si alcanzamos el umbral, realizar limpieza
    if event_counter_since_reset >= RESET_THRESHOLD:
        await reset_database_keep_last()

async def reset_database_keep_last():
    """Conserva solo los últimos KEEP_AFTER_RESET eventos, reinicia conteos y nivel actual."""
    global spaceman_history, spaceman_events_seen, level_counts, current_level, event_counter_since_reset
    logger.warning(f"Se alcanzaron {event_counter_since_reset} eventos. Iniciando limpieza de base de datos...")

    async with aiosqlite.connect(DB_PATH) as db:
        # Obtener los últimos KEEP_AFTER_RESET eventos ordenados por timestamp descendente
        async with db.execute(
            'SELECT id, maxMultiplier, timestamp_recepcion, nivel FROM events ORDER BY timestamp_recepcion DESC LIMIT ?',
            (KEEP_AFTER_RESET,)
        ) as cursor:
            rows = await cursor.fetchall()

        if not rows:
            logger.warning("No hay eventos para conservar. Limpieza cancelada.")
            event_counter_since_reset = 0
            return

        # Limpiar tabla events
        await db.execute('DELETE FROM events')
        # Reinsertar los eventos conservados
        for row in rows:
            await db.execute(
                'INSERT INTO events (id, maxMultiplier, timestamp_recepcion, nivel) VALUES (?, ?, ?, ?)',
                row
            )

        # Limpiar tabla counts
        await db.execute('DELETE FROM counts')

        # Recalcular level_counts a partir de los eventos conservados
        new_level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})
        for row in rows:
            multiplier = row[1]
            nivel = row[3]
            range_key = None
            if 3.00 <= multiplier <= 4.99:
                range_key = '3-4.99'
            elif 5.00 <= multiplier <= 9.99:
                range_key = '5-9.99'
            elif multiplier >= 10.00:
                range_key = '10+'
            if range_key:
                new_level_counts[nivel][range_key] += 1

        # Insertar nuevos conteos en la tabla counts
        for level, ranges in new_level_counts.items():
            for rng, cnt in ranges.items():
                await db.execute(
                    'INSERT INTO counts (level, range, count) VALUES (?, ?, ?)',
                    (level, rng, cnt)
                )

        # El nivel actual debe ser el del evento más reciente (el primero de rows, pues están ordenados DESC)
        new_current_level = rows[0][3] if rows else 0
        await db.execute(
            'INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)',
            ('current_level', str(new_current_level))
        )
        await db.commit()

        # Actualizar variables en memoria
        spaceman_history = []
        spaceman_events_seen.clear()
        for row in rows[:MAX_HISTORY]:  # Solo los primeros MAX_HISTORY para el historial en memoria
            event = {
                'event_id': row[0],
                'maxMultiplier': row[1],
                'timestamp_recepcion': row[2],
                'nivel': row[3]
            }
            spaceman_history.append(event)
            spaceman_events_seen.add(row[0])

        level_counts = new_level_counts
        current_level = new_current_level
        event_counter_since_reset = len(rows)

    logger.info(f"Limpieza completada. Conservados {len(rows)} eventos. Nivel actual: {current_level}")

async def update_count(level: int, range_key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO counts (level, range, count) VALUES (?, ?, 1)
            ON CONFLICT(level, range) DO UPDATE SET count = count + 1
        ''', (level, range_key))
        await db.commit()

async def update_current_level(level: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)
        ''', ('current_level', str(level)))
        await db.commit()

# ============================================
# AUTO‑PING
# ============================================
async def self_ping():
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        logger.info("[PING] Auto‑ping exitoso, servicio activo")
                    else:
                        logger.warning(f"[PING] Auto‑ping falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error en auto‑ping: {e}")

# ============================================
# BATCH SENDER
# ============================================
async def batch_sender():
    pending_events = []
    while True:
        try:
            event = await asyncio.wait_for(event_queue.get(), timeout=BATCH_TIMEOUT)
            pending_events.append(event)
            if len(pending_events) >= BATCH_SIZE:
                await send_batch(pending_events.copy())
                pending_events.clear()
        except asyncio.TimeoutError:
            if pending_events:
                await send_batch(pending_events.copy())
                pending_events.clear()
        except Exception as e:
            logger.error(f"Error en batch_sender: {e}")

async def send_batch(events_list: List[dict]):
    if not connected_clients:
        return
    batch_msg = {
        'tipo': 'batch',
        'eventos': events_list
    }
    message = json.dumps(batch_msg, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )
    logger.info(f"Enviado lote de {len(events_list)} eventos")

# ============================================
# PERIODIC TABLE SENDER
# ============================================
async def periodic_table_sender():
    while True:
        interval = random.uniform(TABLE_UPDATE_MIN, TABLE_UPDATE_MAX)
        await asyncio.sleep(interval)
        if not connected_clients:
            continue
        table_msg = {
            'tipo': 'nivel_counts',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()}
        }
        message = json.dumps(table_msg, default=str)
        await asyncio.gather(
            *[client.send_str(message) for client in connected_clients],
            return_exceptions=True
        )
        logger.info(f"Tabla de niveles enviada (intervalo {interval:.1f}s)")

# ============================================
# MONITOREO SPACEMAN
# ============================================
async def monitor_spaceman():
    global current_level, spaceman_last_multiplier, spaceman_history, level_counts, _last_gen_time, _gen_counter
    reconnect_delay = BASE_RECONNECT_DELAY
    logger.info("[SPACEMAN] 🚀 Iniciando monitor")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    logger.info("[SPACEMAN] ✅ WebSocket conectado")
                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    }
                    await ws.send_json(subscribe_msg)
                    logger.info("[SPACEMAN] 📡 Suscripción enviada")
                    reconnect_delay = BASE_RECONNECT_DELAY

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                                if "gameResult" in data and data["gameResult"]:
                                    result_str = data["gameResult"][0].get("result")
                                    if result_str:
                                        multiplier = float(result_str)
                                        if multiplier >= 1.00 and multiplier != spaceman_last_multiplier:
                                            spaceman_last_multiplier = multiplier

                                            # Intentar obtener ID real
                                            game_id = None
                                            if "gameId" in data:
                                                game_id = data["gameId"]
                                            elif "roundId" in data:
                                                game_id = data["roundId"]
                                            elif "id" in data:
                                                game_id = data["id"]
                                            elif "gameResult" in data and data["gameResult"]:
                                                first_result = data["gameResult"][0]
                                                if "gameId" in first_result:
                                                    game_id = first_result["gameId"]
                                                elif "roundId" in first_result:
                                                    game_id = first_result["roundId"]
                                                elif "id" in first_result:
                                                    game_id = first_result["id"]

                                            if not game_id:
                                                now = time.time()
                                                if now == _last_gen_time:
                                                    _gen_counter += 1
                                                else:
                                                    _last_gen_time = now
                                                    _gen_counter = 0
                                                ts_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                                                game_id = f"spaceman_{ts_str}_{_gen_counter}_{multiplier:.2f}"
                                                logger.warning(f"[SPACEMAN] ⚠️ ID generado: {game_id}")

                                            if game_id not in spaceman_events_seen:
                                                spaceman_events_seen.add(game_id)

                                                # Actualizar nivel
                                                if multiplier < 2.00:
                                                    current_level -= 1
                                                else:
                                                    current_level += 1

                                                # Rango
                                                range_key = None
                                                if 3.00 <= multiplier <= 4.99:
                                                    range_key = '3-4.99'
                                                elif 5.00 <= multiplier <= 9.99:
                                                    range_key = '5-9.99'
                                                elif multiplier >= 10.00:
                                                    range_key = '10+'

                                                evento = {
                                                    'tipo': 'spaceman',
                                                    'event_id': game_id,
                                                    'maxMultiplier': multiplier,
                                                    'timestamp_recepcion': datetime.now().isoformat(),
                                                    'nivel': current_level
                                                }

                                                # Memoria (últimos 100)
                                                spaceman_history.insert(0, evento)
                                                if len(spaceman_history) > MAX_HISTORY:
                                                    spaceman_history.pop()
                                                if range_key:
                                                    level_counts[current_level][range_key] += 1

                                                # Base de datos (con control de reset automático)
                                                await save_event(evento)
                                                if range_key:
                                                    await update_count(current_level, range_key)
                                                await update_current_level(current_level)

                                                logger.info(f"[SPACEMAN] 🚀 NUEVO: GameID={game_id} | {multiplier:.2f}x | Nivel={current_level}")
                                                await event_queue.put(evento)
                                            else:
                                                logger.info(f"[SPACEMAN] ⚠️ Duplicado: GameID={game_id} | {multiplier:.2f}x")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError) as e:
                                logger.debug(f"[SPACEMAN] Error procesando mensaje: {e}")
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.info("[SPACEMAN] 🔌 Conexión cerrada")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"[SPACEMAN] ❌ Error: {ws.exception()}")
                            break
        except Exception as e:
            logger.error(f"[SPACEMAN] 💥 {e}, reconexión en {reconnect_delay:.1f}s")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_RECONNECT_DELAY, reconnect_delay * 2)

# ============================================
# SERVIDOR HTTP + WEBSOCKET
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        if spaceman_history:
            await ws.send_json({
                'tipo': 'historial',
                'api': 'spaceman',
                'eventos': spaceman_history
            })
        await ws.send_json({
            'tipo': 'nivel_counts',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()}
        })
        logger.info("Cliente Spaceman conectado, historial y tabla de niveles enviados")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor Spaceman activo. Use /ws para WebSocket o /health para health check.", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', root_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"✅ Servidor Spaceman escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor Spaceman con reset automático cada 1000 eventos (conserva últimos 100)")
    logger.info("=" * 60)
    await init_db()
    await load_from_db()
    asyncio.create_task(batch_sender())
    asyncio.create_task(periodic_table_sender())
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_spaceman()),
        asyncio.create_task(self_ping()),
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
