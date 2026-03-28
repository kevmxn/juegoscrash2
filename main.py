#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SLIDE con servidor HTTP y WebSocket
- Polling a la API de Stake Slide con headers sin Brotli
- Envía solo los últimos 20 eventos a nuevos clientes (en fragmentos de 200)
- Broadcast de nuevos eventos de Slide en lotes (máx 300)
- Backoff exponencial y circuit breaker
- Auto‑ping cada 10 minutos para evitar que Render suspenda el servicio
- Timestamps en horario de Argentina
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import logging
import os
from datetime import datetime
from typing import Set, Dict, Any, List
from zoneinfo import ZoneInfo  # Python 3.9+

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN SLIDE
# ============================================
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.76',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0',
    'Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300

slide_ids: Set[str] = set()
slide_status = {'consecutive_errors': 0, 'next_allowed_time': 0, 'blocked_until': 0}
slide_history: List[Dict] = []
MAX_HISTORY = 15000

connected_clients: Set[web.WebSocketResponse] = set()
client_semaphore = asyncio.Semaphore(10)

# Batching settings
BATCH_SIZE = 300
BATCH_FLUSH_INTERVAL = 2.0
event_batch: List[Dict] = []
batch_lock = asyncio.Lock()

# History chunking settings
HISTORY_CHUNK_SIZE = 200
HISTORY_SEND_LIMIT = 20

# Zona horaria Argentina
AR_TZ = ZoneInfo('America/Argentina/Buenos_Aires')

def now_argentina() -> str:
    """Retorna timestamp actual en horario de Argentina como ISO 8601."""
    return datetime.now(AR_TZ).isoformat()

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
                        logger.info("[PING] Auto‑ping exitoso")
                    else:
                        logger.warning(f"[PING] Falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error: {e}")

# ============================================
# FUNCIONES SLIDE
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_slide(session: aiohttp.ClientSession) -> dict | None:
    now = time.time()
    if now < slide_status['blocked_until']:
        wait = slide_status['blocked_until'] - now
        logger.info(f"[SLIDE] 🚫 Bloqueado por {wait:.1f}s")
        await asyncio.sleep(wait)
        return None
    if now < slide_status['next_allowed_time']:
        wait = slide_status['next_allowed_time'] - now
        logger.info(f"[SLIDE] ⏳ Backoff {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        async with session.get(API_SLIDE, headers=headers, timeout=10) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                slide_status['next_allowed_time'] = time.time() + retry_after
                slide_status['consecutive_errors'] += 1
                logger.warning(f"[SLIDE] ⚠️ Esperar {retry_after}s (Retry-After)")
                return None
            if resp.status == 200:
                slide_status['consecutive_errors'] = 0
                return await resp.json()
            elif resp.status == 403:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                logger.warning(f"[SLIDE] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[SLIDE] 🔒 Bloqueado {BLOCK_TIME}s")
                return None
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + retry_after
                slide_status['consecutive_errors'] += 1
                logger.warning(f"[SLIDE] ⚠️ Rate limit, esperar {retry_after}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
            elif 500 <= resp.status < 600:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                logger.error(f"[SLIDE] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
            else:
                logger.warning(f"[SLIDE] ⚠️ Código inesperado: {resp.status}")
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
    except asyncio.TimeoutError:
        slide_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
        slide_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[SLIDE] ⏰ Timeout, backoff {backoff:.1f}s")
        if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            slide_status['blocked_until'] = time.time() + BLOCK_TIME
        return None
    except Exception as e:
        slide_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
        slide_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[SLIDE] 💥 Excepción: {e}")
        if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            slide_status['blocked_until'] = time.time() + BLOCK_TIME
        return None

async def procesar_slide(data: dict):
    global slide_history
    event_id = data.get('id')
    if not event_id or event_id in slide_ids:
        return
    slide_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    started_at = data_inner.get('startedAt')
    if max_mult is not None and max_mult > 0:
        evento = {
            'event_id': event_id,
            'maxMultiplier': max_mult,
            'startedAt': started_at,
            'timestamp_recepcion': now_argentina()
        }
        slide_history.insert(0, evento)
        if len(slide_history) > MAX_HISTORY:
            slide_history.pop()
        logger.info(f"[SLIDE] ✅ NUEVO: ID={event_id} | {max_mult}x | Inicio={started_at}")

        await add_to_batch({
            'tipo': 'slide',
            'id': event_id,
            'maxMultiplier': max_mult,
            'startedAt': started_at,
            'timestamp_recepcion': evento['timestamp_recepcion']
        })
    else:
        logger.warning(f"[SLIDE] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_slide():
    logger.info("[SLIDE] 🚀 Iniciando monitor")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_slide(session)
            if data:
                await procesar_slide(data)
            await asyncio.sleep(random.uniform(0.5, 1.5))

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
        'api': 'slide',
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
# HISTORY CHUNKING (solo últimos 20 eventos)
# ============================================
async def send_history_in_chunks(ws: web.WebSocketResponse, history: List[Dict], limit: int = HISTORY_SEND_LIMIT):
    """Envía solo los últimos `limit` eventos, en fragmentos de HISTORY_CHUNK_SIZE."""
    limited_history = history[:limit]   # tomar los más recientes (primeros en la lista)
    total = len(limited_history)
    if total == 0:
        await ws.send_json({'tipo': 'historial_start', 'total': 0})
        await ws.send_json({'tipo': 'historial_end'})
        return

    await ws.send_json({'tipo': 'historial_start', 'total': total})

    for i in range(0, total, HISTORY_CHUNK_SIZE):
        chunk = limited_history[i:i + HISTORY_CHUNK_SIZE]
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
            await send_history_in_chunks(ws, slide_history)
            logger.info(f"Cliente Slide conectado, enviados últimos {HISTORY_SEND_LIMIT} eventos")
            async for msg in ws:
                if msg.type == web.WSMsgType.CLOSE:
                    break
        finally:
            connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor Slide activo. Use /ws para WebSocket o /health para health check.", status=200)

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
    logger.info(f"✅ Servidor Slide escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor exclusivo de SLIDE con WebSocket, batching y solo últimos 20 eventos")
    logger.info("=" * 60)
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_slide()),
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
