#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SPACEMAN con servidor HTTP y WebSocket
- Conecta al WebSocket de Pragmatic Play
- Envía historial a clientes WebSocket al conectar
- Broadcast de nuevos eventos de Spaceman en lotes (máx 300)
- Reconexión automática con backoff exponencial
- Auto‑ping cada 10 minutos para evitar que Render suspenda el servicio
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

spaceman_last_multiplier: float = None
spaceman_events_seen: Set[str] = set()
spaceman_history: List[Dict] = []
MAX_HISTORY = 15000

connected_clients: Set[web.WebSocketResponse] = set()

# Batching settings
BATCH_SIZE = 300
BATCH_FLUSH_INTERVAL = 2.0
event_batch: List[Dict] = []
batch_lock = asyncio.Lock()

# ============================================
# AUTO‑PING PARA MANTENER EL SERVICIO ACTIVO
# ============================================
async def self_ping():
    """Hace una petición a /health cada 10 minutos para evitar que Render suspenda el servicio."""
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while True:
        await asyncio.sleep(600)  # 10 minutos
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
# MONITOREO SPACEMAN
# ============================================
async def monitor_spaceman():
    global spaceman_last_multiplier, spaceman_history
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
                                            game_id = data.get("gameId", "unknown")
                                            if game_id not in spaceman_events_seen:
                                                spaceman_events_seen.add(game_id)
                                                evento = {
                                                    'event_id': game_id,
                                                    'maxMultiplier': multiplier,
                                                    'timestamp_recepcion': datetime.now().isoformat()
                                                }
                                                spaceman_history.insert(0, evento)
                                                if len(spaceman_history) > MAX_HISTORY:
                                                    spaceman_history.pop()
                                                logger.info(f"[SPACEMAN] 🚀 NUEVO: GameID={game_id} | {multiplier:.2f}x")
                                                await add_to_batch({
                                                    'tipo': 'spaceman',
                                                    'id': game_id,
                                                    'maxMultiplier': multiplier,
                                                    'timestamp_recepcion': evento['timestamp_recepcion']
                                                })
                                            else:
                                                logger.info(f"[SPACEMAN] ⚠️ Duplicado: GameID={game_id} | {multiplier:.2f}x")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                                pass
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
# BATCH MANAGEMENT
# ============================================
async def add_to_batch(event: Dict[str, Any]):
    """Add an event to the batch and flush if size threshold reached."""
    global event_batch
    async with batch_lock:
        event_batch.append(event)
        if len(event_batch) >= BATCH_SIZE:
            await flush_batch()

async def flush_batch():
    """Send current batch to all connected clients and clear it."""
    global event_batch
    async with batch_lock:
        if not event_batch:
            return
        batch_copy = event_batch[:]
        event_batch.clear()
    # Broadcast the batch
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
    """Periodically flush the batch even if not full."""
    while True:
        await asyncio.sleep(BATCH_FLUSH_INTERVAL)
        await flush_batch()

# ============================================
# SERVIDOR HTTP + WEBSOCKET (para clientes)
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
        logger.info("Cliente Spaceman conectado, historial enviado")
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
    logger.info("🚀 Monitor exclusivo de SPACEMAN con WebSocket, batching y auto‑ping")
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
