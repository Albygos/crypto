import asyncio, json, os, re, sqlite3, time
from datetime import datetime, timezone
from urllib.parse import urlparse
import httpx
import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

WS_URL = os.getenv("PUMPPORTAL_WS_URL", "wss://pumpportal.fun/api/data")
DEX_URL = "https://api.dexscreener.com/latest/dex/tokens/"
ONLY_FUN = os.getenv("ONLY_FUN", "true").lower() == "true"
ENRICH_SECONDS = float(os.getenv("ENRICH_SECONDS", "2"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))
DB_PATH = os.getenv("DB_PATH", "/tmp/tokens.db")

app = FastAPI(title="Pump.fun New Token Website Scanner")
app.mount("/static", StaticFiles(directory="static"), name="static")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("""CREATE TABLE IF NOT EXISTS tokens(
 mint TEXT PRIMARY KEY, name TEXT, symbol TEXT, website TEXT,
 market_cap REAL, volume REAL, created_ts REAL, pump_url TEXT,
 first_seen REAL, source TEXT)""")
conn.commit()

tokens={}
last_event=0
event_count=0
lock=asyncio.Lock()

def norm_url(v):
    if not isinstance(v,str) or not v.strip(): return ""
    v=v.strip()
    if not v.startswith(("http://","https://")): v="https://"+v
    try:
        u=urlparse(v)
        if not u.hostname: return ""
        return v
    except: return ""

def is_fun(v):
    try:
        h=(urlparse(v).hostname or "").lower().rstrip(".")
        return h.endswith(".fun")
    except: return False

def find_urls(obj):
    found=[]
    def walk(x):
        if isinstance(x,dict):
            for k,v in x.items():
                kl=str(k).lower()
                if isinstance(v,str) and any(a in kl for a in ("website","site","url","uri","metadata")):
                    u=norm_url(v)
                    if u: found.append(u)
                walk(v)
        elif isinstance(x,list):
            for v in x: walk(v)
        elif isinstance(x,str):
            for m in re.findall(r'https?://[^\s"<>]+',x):
                u=norm_url(m.rstrip("),.;"))
                if u: found.append(u)
    walk(obj)
    # Prefer .fun
    for u in found:
        if is_fun(u): return u
    return found[0] if found else ""

async def dex_enrich(client,mint):
    try:
        r=await client.get(DEX_URL+mint,timeout=8)
        if r.status_code != 200: return {}
        data=r.json()
        pairs=data.get("pairs") or []
        if not pairs: return {}
        p=max(pairs,key=lambda x: float((x.get("liquidity") or {}).get("usd") or 0))
        info=p.get("info") or {}
        websites=info.get("websites") or []
        social=[]
        for s in info.get("socials") or []:
            if isinstance(s,dict): social.append(s.get("url",""))
        website=next((norm_url(x.get("url")) for x in websites if isinstance(x,dict) and norm_url(x.get("url"))), "")
        if not website:
            for x in social:
                if is_fun(x): website=x; break
        return {
          "website":website,
          "market_cap":float(p.get("marketCap") or p.get("fdv") or 0),
          "volume":float((p.get("volume") or {}).get("h24") or 0)
        }
    except Exception:
        return {}

async def add_token(raw, client):
    global last_event,event_count
    mint=raw.get("mint") or raw.get("tokenAddress")
    if not mint: return
    website=find_urls(raw)
    enrich={}
    # New launch may not yet have a DEX pair. Retry enrichment briefly.
    if not website:
        await asyncio.sleep(ENRICH_SECONDS)
        enrich=await dex_enrich(client,mint)
        website=enrich.get("website","")
    if ONLY_FUN and not is_fun(website):
        return
    created=raw.get("created_timestamp") or raw.get("timestamp") or time.time()
    if isinstance(created,str):
        try: created=datetime.fromisoformat(created.replace("Z","+00:00")).timestamp()
        except: created=time.time()
    if created>10_000_000_000: created/=1000
    t={
      "mint":mint,
      "name":raw.get("name") or "Unknown",
      "symbol":raw.get("symbol") or "",
      "website":website,
      "market_cap":float(enrich.get("market_cap") or raw.get("marketCap") or raw.get("usd_market_cap") or 0),
      "volume":float(enrich.get("volume") or raw.get("volume") or 0),
      "created_ts":float(created),
      "pump_url":f"https://pump.fun/coin/{mint}",
      "first_seen":time.time(),
      "source":"PumpPortal WebSocket"
    }
    async with lock:
        tokens[mint]=t
        conn.execute("""INSERT OR REPLACE INTO tokens
        (mint,name,symbol,website,market_cap,volume,created_ts,pump_url,first_seen,source)
        VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (t["mint"],t["name"],t["symbol"],t["website"],t["market_cap"],t["volume"],
         t["created_ts"],t["pump_url"],t["first_seen"],t["source"]))
        conn.commit()
        last_event=time.time(); event_count+=1

async def pump_stream():
    while True:
        try:
            async with httpx.AsyncClient(headers={"User-Agent":"Mozilla/5.0"}) as client:
                async with websockets.connect(WS_URL,ping_interval=20,ping_timeout=20,max_size=4_000_000) as ws:
                    await ws.send(json.dumps({"method":"subscribeNewToken"}))
                    print("Connected to PumpPortal WebSocket",flush=True)
                    async for msg in ws:
                        try:
                            raw=json.loads(msg)
                            asyncio.create_task(add_token(raw,client))
                        except Exception as e:
                            print("Event parse error",repr(e),flush=True)
        except Exception as e:
            print("WebSocket disconnected:",repr(e),flush=True)
            await asyncio.sleep(3)

task=None
@app.on_event("startup")
async def startup():
    global task
    task=asyncio.create_task(pump_stream())

@app.on_event("shutdown")
async def shutdown():
    if task: task.cancel()
    conn.close()

@app.get("/")
async def home(): return FileResponse("static/index.html")

@app.get("/api/tokens")
async def api_tokens():
    async with lock:
        rows=sorted(tokens.values(),key=lambda x:x["first_seen"],reverse=True)[:MAX_ITEMS]
    return {"tokens":rows,"last_event":last_event,"event_count":event_count,"only_fun":ONLY_FUN}

@app.get("/health")
async def health():
    return {"ok":True,"websocket_connected_recently":bool(last_event and time.time()-last_event<60),"last_event":last_event}
