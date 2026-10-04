# Pump.fun New Token Website Scanner — Live WebSocket Edition

This version does NOT depend on Pump.fun's `frontend-api-v3.pump.fun/coins/latest` REST endpoint.

## Data architecture

1. PumpPortal WebSocket receives `subscribeNewToken` events for new token creations.
2. The token event is inspected for website/metadata URLs.
3. If needed, the service briefly enriches the mint through DexScreener.
4. The website is filtered to `.fun` by default.
5. Matching tokens are shown live in the browser.

PumpPortal's public WebSocket documentation describes `wss://pumpportal.fun/api/data` and the `subscribeNewToken` subscription.

DexScreener is used only as optional enrichment for market cap/volume and metadata after a token is discovered.

## Deploy on Render

Use:
- Build: `pip install -r requirements.txt`
- Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`

`render.yaml` already contains these settings.

## Environment variables

`ONLY_FUN=true` — only show `.fun` websites.
`ENRICH_SECONDS=2` — wait before optional DEX enrichment.
`MAX_ITEMS=200` — maximum tokens kept in memory.

## Important limitations

No external API can guarantee that every token has a website immediately at creation time. Metadata may arrive late or be absent. A token can therefore appear only if its website can be discovered from the event/metadata or subsequent enrichment.

PumpPortal and DexScreener are third-party services and can change rate limits, availability, or API behavior. This scanner automatically reconnects its WebSocket after disconnection.
