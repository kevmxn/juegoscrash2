#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SPACEMAN con servidor HTTP y WebSocket
- Conecta al WebSocket de Pragmatic Play
- Envía historial fragmentado y lotes de eventos
- Reconexión automática con backoff exponencial
- Auto‑ping cada 10 minutos para evitar suspensión
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import logging
import os
from datetime import datetime
from typing import Set, Dict, Any, List

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

BASE_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0

spaceman_history: List[Dict] = []
MAX_HISTORY = 15000

connected_clients: Set[web.WebSocketResponse] = set()
client_semaphore = asyncio.Semaphore(10)

# Batching settings
BATCH_SIZE = 300
BATCH_FLUSH_INTERVAL = 2.0
event_batch: List[Dict] = []
batch_lock = asyncio.Lock()

# History chunking
HISTORY_CHUNK_SIZE = 500

# ============================================
# AUTO‑PING (para mantener el servicio activo)
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
                        logger.info("[PING] Auto‑ping exitoso")
                    else:
                        logger.warning(f"[PING] Falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error: {e}")

# ============================================
# FUNCIONES AUXILIARES
# ============================================
def extract_event_id(data: dict) -> str:
    """Intenta extraer un ID único del mensaje."""
    # Lista de campos comunes en orden de prioridad
    for key in ['roundId', 'gameId', 'id', 'eventId', 'uid', 'externalId']:
        if key in data and data[key]:
            val = str(data[key])
            # Si parece una fecha, la ignoramos (evita falsos IDs)
            if val.startswith('20') and '-' in val[:10]:
                continue
            return val
    # Si hay un campo 'gameResult', podría contener el ID
    if 'gameResult' in data and isinstance(data['gameResult'], list) and len(data['gameResult']) > 0:
        gr = data['gameResult'][0]
        for key in ['roundId', 'gameId', 'id']:
            if key in gr and gr[key]:
                return str(gr[key])
    # Último recurso: generar ID basado en timestamp y multiplicador
    mult = extract_multiplier(data)
    ts = datetime.now().timestamp()
    return f"ev_{int(ts)}_{mult}" if mult is not None else f"ev_{int(ts)}"

def extract_multiplier(data: dict) -> float | None:
    """Extrae el multiplicador del mensaje."""
    # Ruta principal: gameResult[0].result
    if 'gameResult' in data:
        gr = data['gameResult']
        if isinstance(gr, list) and len(gr) > 0:
            res = gr[0].get('result')
            if res:
                try:
                    return float(res)
                except (ValueError, TypeError):
                    pass
        elif isinstance(gr, dict):
            res = gr.get('result')
            if res:
                try:
                    return float(res)
                except (ValueError, TypeError):
                    pass
    # Campos alternativos
    for key in ['multiplier', 'result', 'crashPoint']:
        if key in data:
            try:
                return float(data[key])
            except (ValueError, TypeError):
                pass
    return None

# ============================================
# MONITOR SPACEMAN
# ============================================
async def monitor_spaceman():
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
                                # Log opcional para depuración (puedes descomentarlo)
                                # logger.debug(f"[SPACEMAN] Mensaje: {json.dumps(data)[:500]}")

                                # Responder a pings si el servidor los envía como mensajes de texto
                                if data.get('type') == 'ping':
                                    await ws.send_json({'type': 'pong'})
                                    continue

                                multiplier = extract_multiplier(data)
                                if multiplier is not None and multiplier >= 1.00:
                                    event_id = extract_event_id(data)
                                    # Verificar si ya existe en el historial
                                    exists = any(e['event_id'] == event_id for e in spaceman_history[:100])
                                    if not exists:
                                        evento = {
                                            'event_id': event_id,
                                            'maxMultiplier': multiplier,
                                            'timestamp_recepcion': datetime.now().isoformat()
                                        }
                                        spaceman_history.insert(0, evento)
                                        if len(spaceman_history) > MAX_HISTORY:
                                            spaceman_history.pop()
                                        logger.info(f"[SPACEMAN] 🚀 NUEVO: ID={event_id} | {multiplier:.2f}x")
                                        await add_to_batch({
                                            'tipo': 'spaceman',
                                            'id': event_id,
                                            'maxMultiplier': multiplier,
                                            'timestamp_recepcion': evento['timestamp_recepcion']
                                        })
                                    else:
                                        logger.info(f"[SPACEMAN] ⚠️ Duplicado ignorado: ID={event_id} | {multiplier:.2f}x")
                                else:
                                    # Mensaje sin multiplicador válido (podría ser heartbeat)
                                    logger.debug(f"[SPACEMAN] Mensaje sin multiplicador: {data.get('type', 'desconocido')}")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError) as e:
                                logger.error(f"[SPACEMAN] Error parseando mensaje: {e}")
                                logger.debug(f"Contenido problemático: {msg.data[:200]}")
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.info("[SPACEMAN] 🔌 Conexión cerrada por el servidor")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"[SPACEMAN] ❌ Error WebSocket: {ws.exception()}")
                            break
        except Exception as e:
            logger.error(f"[SPACEMAN] 💥 Excepción: {e}, reconexión en {reconnect_delay:.1f}s")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_RECONNECT_DELAY, reconnect_delay * 2)

# ============================================
# BATCH MANAGEMENT
# ============================================
async def add_to_batch(event: Dict[str, Any]):
    global event_batch
    async with batch_lock:
        event_batch.append(event)
        if len(event_batch) >= BATCH_SIZE:
            await flush_batch()

async def flush_batch():
    global event_batch
    async with batch_lock:
        if not event_batch:
            return
        batch_copy = event_batch[:]
        event_batch.clear()
    if not connected_clients:
        return
    message = json.dumps({
        'tipo': 'batch',
        'api': 'spaceman',
        'eventos': batch_copy
    }, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )
    logger.debug(f"Batch flusheado: {len(batch_copy)} eventos")

async def batch_flusher():
    while True:
        await asyncio.sleep(BATCH_FLUSH_INTERVAL)
        await flush_batch()

# ============================================
# HISTORY CHUNKING
# ============================================
async def send_history_in_chunks(ws: web.WebSocketResponse, history: List[Dict]):
    total = len(history)
    if total == 0:
        await ws.send_json({'tipo': 'historial_start', 'total': 0})
        await ws.send_json({'tipo': 'historial_end'})
        return

    await ws.send_json({'tipo': 'historial_start', 'total': total})

    for i in range(0, total, HISTORY_CHUNK_SIZE):
        chunk = history[i:i + HISTORY_CHUNK_SIZE]
        await ws.send_json({
            'tipo': 'historial_chunk',
            'offset': i,
            'chunk': chunk
        })
        await asyncio.sleep(0.01)

    await ws.send_json({'tipo': 'historial_end'})

# ============================================
# SERVIDOR HTTP + WEBSOCKET
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async with client_semaphore:
        connected_clients.add(ws)
        try:
            await send_history_in_chunks(ws, spaceman_history)
            logger.info("Cliente Spaceman conectado, historial enviado en fragmentos")
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
    logger.info("🚀 Monitor exclusivo de SPACEMAN con WebSocket, batching y fragmentación")
    logger.info("=" * 60)
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_spaceman()),
        asyncio.create_task(self_ping()),
        asyncio.create_task(batch_flusher()),
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
