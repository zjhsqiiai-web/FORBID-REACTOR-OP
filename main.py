#!/usr/bin/env python3
"""
FORBID v4 — Gateway + Presence + Byte-Level Bandwidth Cap.

- Reacts F-O-R-B-I-D to every non-self message in every guild.
- Maintains online presence.
- Tracks every byte on the wire (REST + gateway).
- Hard-stops the client at HARD_CAP_BYTES.

Env:
  DISCORD_TOKEN              required
  PORT                       default 8080
  HARD_CAP_BYTES             default 5_000_000_000
  SOFT_CAP_BYTES             default 4_500_000_000
  PRESENCE_TEXT              default "FORBID"
  PRESENCE_STATUS            online|idle|dnd|invisible
  MAX_ACTIVE_CHANNELS        default 50
  MAX_QUEUE_PER_CHANNEL      default 200
  MAX_MESSAGE_AGE            default 60
  STATS_INTERVAL             default 120
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from collections import OrderedDict
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════

TOKEN             = (os.environ.get("DISCORD_TOKEN") or "").strip()
PORT              = int(os.environ.get("PORT", "8080"))
HARD_CAP_BYTES    = int(os.environ.get("HARD_CAP_BYTES", str(5_000_000_000)))
SOFT_CAP_BYTES    = int(os.environ.get("SOFT_CAP_BYTES", str(4_500_000_000)))
STATS_INTERVAL    = float(os.environ.get("STATS_INTERVAL", "120"))
PRESENCE_TEXT     = os.environ.get("PRESENCE_TEXT", "FORBID")
PRESENCE_STATUS   = os.environ.get("PRESENCE_STATUS", "online").lower()

REACT_LETTERS     = ("🇫", "🇴", "🇷", "🇧", "🇮", "🇩")

# Reactions: Discord allows ~5 burst / channel, then 1/sec refill.
CH_BUCKET_CAP     = 5.0
CH_BUCKET_REFILL  = 1.0

MAX_ACTIVE_CHANNELS = int(os.environ.get("MAX_ACTIVE_CHANNELS", "50"))
MAX_QUEUE_PER_CH    = int(os.environ.get("MAX_QUEUE_PER_CHANNEL", "200"))
MAX_MESSAGE_AGE     = float(os.environ.get("MAX_MESSAGE_AGE", "60"))
DEDUPE_SIZE         = 16384

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("forbid")
for n in ("discord", "discord.gateway", "discord.http", "aiohttp", "aiohttp.access"):
    logging.getLogger(n).setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════════════════════
# BYTE COUNTERS (global — mutated by monkey patches)
# ═══════════════════════════════════════════════════════════════════════════

BW_rest_out = 0
BW_rest_in  = 0
BW_gw_out   = 0
BW_gw_in    = 0

def total_bytes() -> int:
    return BW_rest_out + BW_rest_in + BW_gw_out + BW_gw_in

def human_bytes(n: float) -> str:
    for u in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.2f}{u}"
        n /= 1024
    return f"{n:.2f}PB"

# ═══════════════════════════════════════════════════════════════════════════
# PATCH 1: REST byte counting via aiohttp TraceConfig
# (Installed globally on every ClientSession instance.)
# ═══════════════════════════════════════════════════════════════════════════

async def _tr_start(session, ctx, params):
    n = len(params.method) + len(str(params.url)) + 16
    for k, v in params.headers.items():
        n += len(k) + len(v) + 4
    n += 2
    ctx._req = n
    ctx._resp = 0

async def _tr_req_chunk(session, ctx, params):
    ctx._req += len(params.chunk)

async def _tr_resp_headers(session, ctx, params):
    n = 17
    for k, v in params.headers.items():
        n += len(k) + len(v) + 4
    ctx._resp = n

async def _tr_resp_chunk(session, ctx, params):
    ctx._resp += len(params.chunk)

async def _tr_end(session, ctx, params):
    global BW_rest_out, BW_rest_in
    BW_rest_out += ctx._req
    BW_rest_in  += ctx._resp

_TRACE = aiohttp.TraceConfig()
_TRACE.on_request_start.append(_tr_start)
_TRACE.on_request_chunk_sent.append(_tr_req_chunk)
_TRACE.on_response_headers_received.append(_tr_resp_headers)
_TRACE.on_response_chunk_received.append(_tr_resp_chunk)
_TRACE.on_request_end.append(_tr_end)

_orig_session_init = aiohttp.ClientSession.__init__
def _patched_session_init(self, *a, **kw):
    tc = list(kw.get("trace_configs") or [])
    tc.append(_TRACE)
    kw["trace_configs"] = tc
    _orig_session_init(self, *a, **kw)
aiohttp.ClientSession.__init__ = _patched_session_init

# ═══════════════════════════════════════════════════════════════════════════
# PATCH 2: gateway byte counting (WebSocket layer)
# ═══════════════════════════════════════════════════════════════════════════

_orig_ws_recv = aiohttp.ClientWebSocketResponse.receive
async def _patched_ws_recv(self, *a, **kw):
    global BW_gw_in
    msg = await _orig_ws_recv(self, *a, **kw)
    t = msg.type
    if t in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
        BW_gw_in += len(msg.data)
    return msg
aiohttp.ClientWebSocketResponse.receive = _patched_ws_recv

_orig_ws_send_str = aiohttp.ClientWebSocketResponse.send_str
async def _patched_ws_send_str(self, data, *a, **kw):
    global BW_gw_out
    BW_gw_out += len(data)
    return await _orig_ws_send_str(self, data, *a, **kw)
aiohttp.ClientWebSocketResponse.send_str = _patched_ws_send_str

# Now safe to import discord.
import discord

# ═══════════════════════════════════════════════════════════════════════════
# METRICS
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Metrics:
    started_at: float = field(default_factory=time.time)
    messages_seen: int = 0
    messages_skipped: int = 0
    messages_queued: int = 0
    messages_dropped: int = 0
    reactions_sent: int = 0
    reactions_429: int = 0
    reactions_403: int = 0
    reactions_404: int = 0
    reactions_err: int = 0
    cap_hit: bool = False

M = Metrics()

def stats_str() -> str:
    up = time.time() - M.started_at
    pct = 100.0 * total_bytes() / HARD_CAP_BYTES
    return (
        f"up={up/3600:.2f}h "
        f"bytes={human_bytes(total_bytes())}/{human_bytes(HARD_CAP_BYTES)} ({pct:.2f}%) "
        f"[gw_in={human_bytes(BW_gw_in)} gw_out={human_bytes(BW_gw_out)} "
        f"rest_out={human_bytes(BW_rest_out)} rest_in={human_bytes(BW_rest_in)}] "
        f"seen={M.messages_seen} skip={M.messages_skipped} q={M.messages_queued} "
        f"drop={M.messages_dropped} react={M.reactions_sent} "
        f"429={M.reactions_429} e403={M.reactions_403} e404={M.reactions_404} "
        f"cap={'HIT' if M.cap_hit else 'ok'}"
    )

# ═══════════════════════════════════════════════════════════════════════════
# LRU DEDUPE
# ═══════════════════════════════════════════════════════════════════════════

class LRUSet:
    __slots__ = ("_cap", "_d")
    def __init__(self, cap: int):
        self._cap = cap
        self._d: OrderedDict[int, None] = OrderedDict()
    def add(self, key: int) -> bool:
        if key in self._d:
            self._d.move_to_end(key)
            return False
        self._d[key] = None
        if len(self._d) > self._cap:
            self._d.popitem(last=False)
        return True

SEEN = LRUSet(DEDUPE_SIZE)

# ═══════════════════════════════════════════════════════════════════════════
# CLIENT — no cache, no chunking, no subscriptions
# ═══════════════════════════════════════════════════════════════════════════

client = discord.Client(
    max_messages=0,
    chunk_guilds_at_startup=False,
    guild_subscriptions=False,
)

# ═══════════════════════════════════════════════════════════════════════════
# PER-CHANNEL REACTION WORKER
# ═══════════════════════════════════════════════════════════════════════════

class ChannelWorker:
    __slots__ = ("cid", "queue", "tokens", "last_refill", "task", "_stop")

    def __init__(self, channel_id: int):
        self.cid = channel_id
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_PER_CH)
        self.tokens = CH_BUCKET_CAP
        self.last_refill = time.monotonic()
        self.task: Optional[asyncio.Task] = None
        self._stop = False

    async def _acquire(self):
        while True:
            now = time.monotonic()
            self.tokens = min(
                CH_BUCKET_CAP,
                self.tokens + (now - self.last_refill) * CH_BUCKET_REFILL,
            )
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep((1.0 - self.tokens) / CH_BUCKET_REFILL)

    async def run(self):
        while not self._stop and not M.cap_hit:
            try:
                msg = await asyncio.wait_for(self.queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            try:
                await self._react(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.debug("react err ch=%d: %r", self.cid, e)
            finally:
                self.queue.task_done()
        log.info("worker stopped ch=%d", self.cid)

    async def _react(self, msg: discord.Message):
        for emoji in REACT_LETTERS:
            if M.cap_hit:
                return
            await self._acquire()
            try:
                await msg.add_reaction(emoji)
                M.reactions_sent += 1
            except discord.HTTPException as e:
                s = getattr(e, "status", None)
                if s == 429:
                    M.reactions_429 += 1
                    ra = float(getattr(e, "retry_after", 5.0) or 5.0) + 0.5
                    await asyncio.sleep(ra)
                    # retry same emoji once
                    await self._acquire()
                    try:
                        await msg.add_reaction(emoji)
                        M.reactions_sent += 1
                    except Exception:
                        return
                elif s == 403:
                    M.reactions_403 += 1
                    return
                elif s == 404:
                    M.reactions_404 += 1
                    return
                else:
                    M.reactions_err += 1
                    return
            except (discord.NotFound, discord.Forbidden):
                return

    def stop(self):
        self._stop = True

# ═══════════════════════════════════════════════════════════════════════════
# WORKER POOL — LRU of active channels
# ═══════════════════════════════════════════════════════════════════════════

WORKERS: "OrderedDict[int, ChannelWorker]" = OrderedDict()

def get_worker(channel_id: int) -> ChannelWorker:
    w = WORKERS.get(channel_id)
    if w is not None:
        WORKERS.move_to_end(channel_id)
        return w
    if len(WORKERS) >= MAX_ACTIVE_CHANNELS:
        _, old = WORKERS.popitem(last=False)
        old.stop()
    w = ChannelWorker(channel_id)
    WORKERS[channel_id] = w
    w.task = asyncio.create_task(w.run(), name=f"ch-{channel_id}")
    return w

# ═══════════════════════════════════════════════════════════════════════════
# PRESENCE
# ═══════════════════════════════════════════════════════════════════════════

_STATUS_MAP = {
    "online":    discord.Status.online,
    "idle":      discord.Status.idle,
    "dnd":       discord.Status.dnd,
    "invisible": discord.Status.invisible,
}

async def apply_presence():
    try:
        status = _STATUS_MAP.get(PRESENCE_STATUS, discord.Status.online)
        await client.change_presence(
            status=status,
            activity=discord.Game(name=PRESENCE_TEXT),
        )
        log.info("presence set: status=%s activity=%r", PRESENCE_STATUS, PRESENCE_TEXT)
    except Exception as e:
        log.warning("presence set failed: %r", e)

# ═══════════════════════════════════════════════════════════════════════════
# DISCORD EVENTS
# ═══════════════════════════════════════════════════════════════════════════

@client.event
async def on_ready():
    log.info("ready as %s (id=%s) guilds=%d", client.user, client.user.id, len(client.guilds))
    await apply_presence()

@client.event
async def on_message(message: discord.Message):
    M.messages_seen += 1

    if M.cap_hit:
        M.messages_skipped += 1
        return

    # Skip self
    if message.author.id == client.user.id:
        M.messages_skipped += 1
        return

    # Skip bots
    if getattr(message.author, "bot", False):
        M.messages_skipped += 1
        return

    # Skip DMs (no guild)
    if message.guild is None:
        M.messages_skipped += 1
        return

    # Skip stale (reconnect replays)
    age = time.time() - message.created_at.timestamp()
    if age > MAX_MESSAGE_AGE:
        M.messages_skipped += 1
        return

    # Dedupe
    if not SEEN.add(message.id):
        M.messages_skipped += 1
        return

    # Enqueue
    try:
        w = get_worker(message.channel.id)
    except Exception:
        M.messages_skipped += 1
        return

    try:
        w.queue.put_nowait(message)
        M.messages_queued += 1
    except asyncio.QueueFull:
        M.messages_dropped += 1

# ═══════════════════════════════════════════════════════════════════════════
# CAP WATCHDOG
# ═══════════════════════════════════════════════════════════════════════════

async def cap_watchdog():
    warned_soft = False
    while True:
        await asyncio.sleep(15)
        t = total_bytes()
        if not warned_soft and t >= SOFT_CAP_BYTES:
            warned_soft = True
            log.warning("SOFT CAP reached: %s / %s — reactions slowing",
                        human_bytes(t), human_bytes(HARD_CAP_BYTES))
        if t >= HARD_CAP_BYTES and not M.cap_hit:
            M.cap_hit = True
            log.critical("HARD CAP reached: %s — closing Discord client",
                         human_bytes(t))
            # Stop all workers
            for w in list(WORKERS.values()):
                w.stop()
            # Close the client (kills gateway, stops REST)
            with suppress(Exception):
                await client.close()
            # Do NOT exit — keep health server alive for UptimeRobot.
            # User will see cap_hit in /stats and can raise the cap or trim guilds.
            return

# ═══════════════════════════════════════════════════════════════════════════
# STATS LOGGER
# ═══════════════════════════════════════════════════════════════════════════

async def stats_logger():
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        log.info("STATS | %s", stats_str())

# ═══════════════════════════════════════════════════════════════════════════
# HEALTH SERVER
# ═══════════════════════════════════════════════════════════════════════════

async def health_server():
    async def ok(_r):
        return aiohttp.web.Response(
            text="ok", content_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )
    async def stats(_r):
        return aiohttp.web.Response(
            text=stats_str(), content_type="text/plain",
            headers={"Cache-Control": "no-store"},
        )
    async def bytes_json(_r):
        p = {
            "total": total_bytes(),
            "hard_cap": HARD_CAP_BYTES,
            "soft_cap": SOFT_CAP_BYTES,
            "pct": round(100.0 * total_bytes() / HARD_CAP_BYTES, 3),
            "gw_in": BW_gw_in, "gw_out": BW_gw_out,
            "rest_out": BW_rest_out, "rest_in": BW_rest_in,
            "cap_hit": M.cap_hit,
        }
        return aiohttp.web.Response(
            text=__import__("json").dumps(p),
            content_type="application/json",
            headers={"Cache-Control": "no-store"},
        )

    app = aiohttp.web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/healthz", ok)
    app.router.add_get("/ping", ok)
    app.router.add_get("/stats", stats)
    app.router.add_get("/bytes", bytes_json)

    runner = aiohttp.web.AppRunner(app, access_log=None)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("health server on :%d", PORT)
    await asyncio.Event().wait()

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

async def amain():
    if not TOKEN:
        log.critical("DISCORD_TOKEN missing.")
        sys.exit(1)

    bg = [
        asyncio.create_task(health_server(), name="health"),
        asyncio.create_task(stats_logger(),  name="stats"),
        asyncio.create_task(cap_watchdog(),  name="cap"),
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    client_task = asyncio.create_task(client.start(TOKEN), name="discord")
    stop_task   = asyncio.create_task(stop.wait(), name="stop")

    done, pending = await asyncio.wait(
        {client_task, stop_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if stop.is_set() and not client_task.done():
        log.info("shutdown signal — closing client")
        with suppress(Exception):
            await client.close()
        client_task.cancel()
        with suppress(asyncio.CancelledError):
            await client_task

    for w in list(WORKERS.values()):
        w.stop()
        if w.task:
            w.task.cancel()
    for t in bg:
        t.cancel()
    await asyncio.gather(*bg, return_exceptions=True)
    log.info("FINAL | %s", stats_str())

def main():
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
