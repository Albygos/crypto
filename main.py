import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

WS_URL = os.getenv("PUMPPORTAL_WS_URL", "wss://pumpportal.fun/api/data")
DEX_URL = "https://api.dexscreener.com/latest/dex/tokens/"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "500"))
DB_PATH = os.getenv("DB_PATH", "/tmp/pumpfun_tokens.db")

app = FastAPI(title="Pump.fun 24H Website Scanner")
app.mount("/static", StaticFiles(directory="static"), name="static")

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("""
CREATE TABLE IF NOT EXISTS tokens (
    mint TEXT PRIMARY KEY,
    name TEXT,
    symbol TEXT,
    website TEXT,
    market_cap REAL DEFAULT 0,
    volume REAL DEFAULT 0,
    created_ts REAL,
    pump_url TEXT,
    first_seen REAL,
    source TEXT
)
""")
db.commit()

tokens = {}
last_event = 0
total_events = 0
lock = asyncio.Lock()
stream_task = None


def normalize_url(value):
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = "https://" + value
    try:
        parsed = urlparse(value)
        if not parsed.hostname:
            return ""
        return value
    except Exception:
        return ""


def hostname(url):
    try:
        return (urlparse(url).hostname or "").lower().rstrip(".")
    except Exception:
        return ""


def website_domain(url):
    host = hostname(url)
    if host == "fun" or host.endswith(".fun"):
        return ".fun"
    if host == "com" or host.endswith(".com"):
        return ".com"
    return "other"


def extract_urls(obj):
    found = []

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                key_l = str(key).lower()
                if isinstance(child, str) and any(
                    word in key_l for word in
                    ("website", "web", "site", "url", "uri", "metadata")
                ):
                    url = normalize_url(child)
                    if url:
                        found.append(url)
                walk(child)

        elif isinstance(value, list):
            for child in value:
                walk(child)

        elif isinstance(value, str):
            for match in re.findall(r'https?://[^\s"<>]+', value):
                url = normalize_url(match.rstrip("),.;"))
                if url:
                    found.append(url)

    walk(obj)

    # Prefer actual website-looking links over social links.
    for url in found:
        host = hostname(url)
        if not any(x in host for x in (
            "twitter.com", "x.com", "t.me", "telegram.me",
            "discord.gg", "discord.com"
        )):
            return url

    return found[0] if found else ""


async def enrich_from_dex(client, mint):
    try:
        response = await client.get(
            DEX_URL + mint,
            timeout=8,
            headers={"Accept": "application/json"}
        )

        if response.status_code != 200:
            return {}

        data = response.json()
        pairs = data.get("pairs") or []

        if not pairs:
            return {}

        pair = max(
            pairs,
            key=lambda item: float(
                (item.get("liquidity") or {}).get("usd") or 0
            )
        )

        info = pair.get("info") or {}
        websites = info.get("websites") or []

        website = ""
        for item in websites:
            if isinstance(item, dict):
                candidate = normalize_url(item.get("url"))
                if candidate:
                    website = candidate
                    break

        return {
            "website": website,
            "market_cap": float(
                pair.get("marketCap")
                or pair.get("fdv")
                or 0
            ),
            "volume": float(
                (pair.get("volume") or {}).get("h24") or 0
            )
        }

    except Exception:
        return {}


def parse_created_timestamp(raw):
    value = (
        raw.get("created_timestamp")
        or raw.get("createdAt")
        or raw.get("created_at")
        or raw.get("timestamp")
    )

    if value is None:
        return time.time()

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(
                value.replace("Z", "+00:00")
            ).timestamp()
        except Exception:
            try:
                value = float(value)
            except Exception:
                return time.time()

    try:
        value = float(value)
        if value > 10_000_000_000:
            value /= 1000
        return value
    except Exception:
        return time.time()


async def process_event(raw, client):
    global last_event, total_events

    mint = raw.get("mint") or raw.get("tokenAddress")
    if not mint:
        return

    created_ts = parse_created_timestamp(raw)

    # Only retain launches from the last 24 hours.
    if created_ts < time.time() - 86400:
        return

    website = extract_urls(raw)
    enrichment = {}

    # Newly created tokens often do not have DEX metadata immediately.
    if not website:
        await asyncio.sleep(2)
        enrichment = await enrich_from_dex(client, mint)
        website = enrichment.get("website", "")

    # We need an actual website for this scanner.
    if not website:
        return

    token = {
        "mint": mint,
        "name": raw.get("name") or "Unknown",
        "symbol": raw.get("symbol") or "",
        "website": website,
        "domain": website_domain(website),
        "market_cap": float(
            enrichment.get("market_cap")
            or raw.get("marketCap")
            or raw.get("usd_market_cap")
            or 0
        ),
        "volume": float(
            enrichment.get("volume")
            or raw.get("volume")
            or 0
        ),
        "created_ts": created_ts,
        "pump_url": f"https://pump.fun/coin/{mint}",
        "first_seen": time.time(),
        "source": "PumpPortal WebSocket"
    }

    async with lock:
        tokens[mint] = token

        db.execute("""
        INSERT OR REPLACE INTO tokens
        (mint,name,symbol,website,market_cap,volume,created_ts,
         pump_url,first_seen,source)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            token["mint"],
            token["name"],
            token["symbol"],
            token["website"],
            token["market_cap"],
            token["volume"],
            token["created_ts"],
            token["pump_url"],
            token["first_seen"],
            token["source"]
        ))

        db.commit()
        last_event = time.time()
        total_events += 1


def load_recent_database():
    cutoff = time.time() - 86400
    rows = db.execute("""
        SELECT mint,name,symbol,website,market_cap,volume,
               created_ts,pump_url,first_seen,source
        FROM tokens
        WHERE created_ts >= ?
        ORDER BY created_ts DESC
        LIMIT ?
    """, (cutoff, MAX_ITEMS)).fetchall()

    for row in rows:
        tokens[row[0]] = {
            "mint": row[0],
            "name": row[1],
            "symbol": row[2],
            "website": row[3],
            "domain": website_domain(row[3]),
            "market_cap": row[4],
            "volume": row[5],
            "created_ts": row[6],
            "pump_url": row[7],
            "first_seen": row[8],
            "source": row[9]
        }


async def stream():
    while True:
        try:
            async with httpx.AsyncClient(
                headers={"User-Agent": "Mozilla/5.0"}
            ) as client:

                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=4_000_000
                ) as ws:

                    await ws.send(json.dumps({
                        "method": "subscribeNewToken"
                    }))

                    print(
                        "Connected: PumpPortal new-token WebSocket",
                        flush=True
                    )

                    async for message in ws:
                        try:
                            event = json.loads(message)

                            # Only process token creation events.
                            if isinstance(event, dict):
                                asyncio.create_task(
                                    process_event(event, client)
                                )

                        except Exception as exc:
                            print(
                                "Event error:",
                                repr(exc),
                                flush=True
                            )

        except Exception as exc:
            print(
                "WebSocket disconnected:",
                repr(exc),
                flush=True
            )
            await asyncio.sleep(3)


async def cleanup_loop():
    while True:
        cutoff = time.time() - 86400

        async with lock:
            old = [
                mint for mint, token in tokens.items()
                if token["created_ts"] < cutoff
            ]

            for mint in old:
                tokens.pop(mint, None)

            db.execute(
                "DELETE FROM tokens WHERE created_ts < ?",
                (cutoff,)
            )
            db.commit()

        await asyncio.sleep(60)


@app.on_event("startup")
async def startup():
    global stream_task

    load_recent_database()

    stream_task = asyncio.create_task(stream())
    asyncio.create_task(cleanup_loop())


@app.on_event("shutdown")
async def shutdown():
    if stream_task:
        stream_task.cancel()

    db.close()


@app.get("/")
async def index():
    return FileResponse("static/index.html")


@app.get("/api/tokens")
async def get_tokens():
    cutoff = time.time() - 86400

    async with lock:
        rows = [
            token for token in tokens.values()
            if token["created_ts"] >= cutoff
        ]

        # Latest/newest first.
        rows.sort(
            key=lambda item: item["created_ts"],
            reverse=True
        )

        rows = rows[:MAX_ITEMS]

    return {
        "tokens": rows,
        "last_event": last_event,
        "total_events": total_events,
        "window_hours": 24,
        "sort": "newest_first"
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "last_event": last_event,
        "recent_event": (
            bool(last_event and time.time() - last_event < 90)
        )
    }
