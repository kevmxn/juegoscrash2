#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import logging
import os
from datetime import datetime
from typing import Set, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

BASE_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0

spaceman_last_multiplier: float = None
spaceman_events_seen: Set[str] = set()
spaceman_history: list = []
MAX_HISTORY = 15000
CHUNK_SIZE = 300

connected_clients: Set[web.WebSocketResponse] = set()

async def broadcast(event_data: Dict[str, Any]):
    if not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )

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
                                    result_obj = data["gameResult"][0]
                                    result_str = result_obj.get("result")
                                    if result_str:
                                        multiplier = float(result_str)
                                        if multiplier >= 1.00 and multiplier != spaceman_last_multiplier:
                                            spaceman_last_multiplier = multiplier
                                            game_id = None
                                            if "gameId" in data and data["gameId"]:
                                                game_id = data["gameId"]
                                            elif "id" in data and data["id"]:
                                                game_id = data["id"]
                                            elif "gameId" in result_obj and result_obj["gameId"]:
                                                game_id = result_obj["gameId"]
                                            elif "id" in result_obj and result_obj["id"]:
                                                game_id = result_obj["id"]
                                            if not game_id:
                                                game_id = f"spaceman_{int(time.time())}"
                                                logger.warning(f"[SPACEMAN] No se encontró ID, usando generado: {game_id}")
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
                                                await broadcast({
                                                    'tipo': 'spaceman',
                                                    'id': game_id,
                                                    'maxMultiplier': multiplier,
                                                    'timestamp_recepcion': evento['timestamp_recepcion']
                                                })
                                            else:
                                                logger.info(f"[SPACEMAN] ⚠️ Duplicado: {game_id}")
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

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        if spaceman_history:
            total = len(spaceman_history)
            await ws.send_json({
                'tipo': 'historial_meta',
                'api': 'spaceman',
                'total': total,
                'chunk_size': CHUNK_SIZE
            })
            for i in range(0, total, CHUNK_SIZE):
                chunk = spaceman_history[i:i+CHUNK_SIZE]
                await ws.send_json({
                    'tipo': 'historial_chunk',
                    'api': 'spaceman',
                    'chunk': chunk,
                    'chunk_index': i // CHUNK_SIZE,
                    'total_chunks': (total + CHUNK_SIZE - 1) // CHUNK_SIZE
                })
                await asyncio.sleep(0.01)
        logger.info("Cliente Spaceman conectado, historial enviado en lotes")
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

async def self_ping():
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        logger.debug("[PING] Auto‑ping exitoso")
        except Exception:
            pass

async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor SPACEMAN con envío de historial en lotes (CHUNK=300)")
    logger.info("=" * 60)
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_spaceman()),
        asyncio.create_task(self_ping()),
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
