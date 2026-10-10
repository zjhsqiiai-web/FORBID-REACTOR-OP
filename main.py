import os
import asyncio
import logging
import time

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

# ── Simple counters for /stats (approximate bandwidth estimate) ──
STATS = {"seen": 0, "reacted": 0, "queued": 0, "dropped": 0}
START = time.time()

# ── Per-channel queues so we never fire 6 reactions at once ──
_queues: dict[int, asyncio.Queue] = {}
_workers: dict[int, asyncio.Task] = {}

async def _worker(channel_id: int):
    q = _queues[channel_id]
    while True:
        msg = await q.get()
        for emoji in LETTERS:
            try:
                await msg.add_reaction(emoji)
                STATS["reacted"] += 1
                await asyncio.sleep(1.0)  # 1 react/sec/channel — Discord safe
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
        STATS["queued"] += 1
    except asyncio.QueueFull:
        STATS["dropped"] += 1

@client.event
async def on_ready():
    logging.info(f"online as {client.user} | {len(client.guilds)} guilds")
    try:
        await client.change_presence(activity=discord.Game(name="FORBID"))
    except Exception as e:
        logging.warning(f"presence failed: {e}")

@client.event
async def on_message(message: discord.Message):
    STATS["seen"] += 1
    if message.author.id == client.user.id:
        return
    if message.guild is None:
        return
    if time.time() - message.created_at.timestamp() > 60:
        return
    _enqueue(message)

# ── Health + stats server ──
async def health():
    app = web.Application()

    async def ok(_r):
        return web.Response(text="ok", headers={"Cache-Control": "no-store"})

    async def stats(_r):
        up_h = (time.time() - START) / 3600
        # ~5 KB per reacted message (gateway event + 6 REST calls)
        est_month_gb = (STATS["reacted"] / max(up_h, 0.01) * 720 * 5000) / 1e9
        return web.Response(
            text=(
                f"uptime={up_h:.2f}h "
                f"seen={STATS['seen']} "
                f"reacted={STATS['reacted']} "
                f"queued={STATS['queued']} "
                f"dropped={STATS['dropped']} "
                f"est_month={est_month_gb:.2f}GB"
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
