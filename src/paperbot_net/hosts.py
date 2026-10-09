"""The only hosts the forward runner may contact (public market data; see binance-spot-api-docs)."""

REST_HOSTS = ("api.binance.com", "api-gcp.binance.com", "api1.binance.com", "api2.binance.com",
              "api3.binance.com", "api4.binance.com", "data-api.binance.vision")
# data-api.binance.vision serves only the market-data-only endpoint list, which (2026-09) excludes
# /api/v3/executionRules and /api/v3/referencePrice; with it those references are unavailable and block entries.
MARKET_DATA_ONLY_REST = "data-api.binance.vision"
WS_HOSTS = {"data-stream.binance.vision": 443, "stream.binance.com": 9443}
TELEGRAM_HOST = "api.telegram.org"
