"""Public Binance spot market-data adapters for the PAPER forward runner (Phase 2).

Everything that touches a network or a socket lives in this package; the accepted ``paperbot`` core stays
network-free. Only unauthenticated GET market-data endpoints and public market streams are used. There is no
API key, signer, order endpoint, account endpoint or user-data stream here, and none can be configured.
"""
