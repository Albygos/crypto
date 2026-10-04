# Pump.fun 24-Hour Website Scanner

Render-ready live scanner for newly created Pump.fun tokens.

## Filters

The website provides:

- `.fun`
- `.com`
- `All Websites`

## Token window

Only tokens created during the previous **24 hours** are returned.

The API and UI sort them by:

**Newest → Oldest**

## Data flow

PumpPortal WebSocket
→ new token event
→ website/metadata extraction
→ optional DexScreener enrichment
→ website-domain filter
→ FastAPI
→ browser

## Render

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Health check:

```text
/health
```

## Important

New launches may initially have incomplete metadata. The scanner attempts a short delayed enrichment using DexScreener.

Third-party APIs and WebSocket services can change availability, rate limits, or response formats.
