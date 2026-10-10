import os
import asyncio
import logging
import time

import aiohttp
from aiohttp import web
import discord

TOKEN = os.environ["BOT_TOKENS"]
PORT = int(os.environ.get("PORT", 8080))
LETTERS = ("🇫", "🇴", "🇷", "🇧", "🇮", "🇩")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("discord").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

client = discord.Client(
    max_messages=0,
    chunk_guilds_at_startup=False,
    guild_subscriptions=False,
)

# ── Byte counter (just logs, no cap enforced — see note at bottom) ──
BYTES = {"in": 0, "out": 0}
_start = time.time()

# ── Patch aiohttp to count REST bytes ──
_orig_session_init = aiohttp.ClientSession.__init__
def _patched_init(self, *a, **kw):
    kw.setdefault("trace_configs", []).append(_trace())
    _orig_session_init(self, *a, **kw)
aiohttp.ClientSession.__init__ = _patched_init

def _trace():
    tc = aiohttp.TraceConfig()
    async def start(s, ctx, p):
        ctx.req = len(p.method) + len(str(p.url))
        ctx.resp = 0
    async def req_chunk(s, ctx, p):
        ctx.req += len(p.chunk)
    async def resp_chunk(s, ctx, p):
        ctx.resp += len(p.chunk)
    async def end(s, ctx, p):
        BYTES["out"] += ctx.req
        BYTES["in"] += ctx.resp
    tc.on_request_start.append(start)
    tc.on_request_chunk_sent.append(req_chunk)
    tc.on_response_chunk_received.append(resp_chunk)
    tc.on_request_end.append(end)
    return tc

# ── Patch WebSocket to count gateway bytes ──
_orig_recv = aiohttp.ClientWebSocketResponse.receive
async def _recv(self, *a, **kw):
    msg = await _orig_recv(self, *a, **kw)
    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
        BYTES["in"] += len(msg.data)
    return msg
aiohttp.ClientWebSocketResponse.receive = _recv

_orig_send = aiohttp.ClientWebSocketResponse.send_str
async def _send(self, data, *a, **kw):
    BYTES["out"] += len(data)
    return await _orig_send(self, data, *a, **kw)
aiohttp.ClientWebSocketResponse.send_str = _send

# ── Reaction queue per channel (keeps us from spamming Discord) ──
_queues: dict[int, asyncio.Queue] = {}
_workers: dict[int, asyncio.Task] = {}

async def _worker(channel_id: int):
    q = _queues[channel_id]
    while True:
        msg = await q.get()
        for emoji in LETTERS:
            try:
                await msg.add_reaction(emoji)
                await asyncio.sleep(1.0)  # Discord: 1 reaction/sec/channel
            except discord.HTTPException as e:
                if getattr(e, "status", None) == 429:
                    await asyncio.sleep(float(getattr(e, "retry_after", 5.0)) + 0.5)
                else:
                    break
            except Exception:
                break
        q.task_done()

def _enqueue(message: discord.Message):
    cid = message.channel.id
    if cid not in _queues:
        _queues[cid] = asyncio.Queue(maxsize=200)
        _workers[cid] = asyncio.create_task(_worker(cid))
    try:
        _queues[cid].put_nowait(message)
    except asyncio.QueueFull:
        pass

@client.event
async def on_ready():
    logging.info(f"online as {client.user} | {len(client.guilds)} guilds")
    try:
        await client.change_presence(activity=discord.Game(name="FORBID"))
    except Exception as e:
        logging.warning(f"presence failed: {e}")

@client.event
async def on_message(message: discord.Message):
    if message.author.id == client.user.id:
        return
    if message.guild is None:
        return
    if time.time() - message.created_at.timestamp() > 60:
        return
    _enqueue(message)

# ── Health server for Render ──
async def health():
    app = web.Application()
    async def ok(_r):
        return web.Response(text="ok", headers={"Cache-Control": "no-store"})
    async def stats(_r):
        up = (time.time() - _start) / 3600
        total = BYTES["in"] + BYTES["out"]
        est_month = (total / max(up, 0.01) * 720) / 1e9
        return web.Response(
            text=(
                f"uptime={up:.2f}h "
                f"total={total/1e6:.2f}MB "
                f"est_month={est_month:.2f}GB "
                f"gw_in={BYTES['in']/1e6:.2f}MB "
                f"rest_out={BYTES['out']/1e6:.2f}MB"
            ),
            headers={"Cache-Control": "no-store"},
        )
    app.router.add_get("/", ok)
    app.router.add_get("/healthz", ok)
    app.router.add_get("/stats", stats)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    logging.info(f"health on :{PORT}")

async def main():
    await health()
    await client.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
