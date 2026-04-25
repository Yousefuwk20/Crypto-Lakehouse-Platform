import requests
import json
import os
import logging
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
VOLUME_PATH = "/Volumes/binance_platform/default/raw_data/metadata"
FILE_NAME   = "exchange_info.json"
TARGET_PATH = f"{VOLUME_PATH}/{FILE_NAME}"

REQUEST_TIMEOUT     = 10
MAX_LISTING_WORKERS = 30

# Same list as raw_data_ingestion.py — single source of truth.
TOP_100_SYMBOLS = [
    "BTCUSDT",   "ETHUSDT",   "BNBUSDT",   "XRPUSDT",   "SOLUSDT",
    "ADAUSDT",   "DOGEUSDT",  "AVAXUSDT",  "LINKUSDT",   "DOTUSDT",
    "TRXUSDT",   "MATICUSDT", "LTCUSDT",   "BCHUSDT",    "NEARUSDT",
    "UNIUSDT",   "ATOMUSDT",  "ETCUSDT",   "FILUSDT",    "ICPUSDT",
    "APTUSDT",   "ARBUSDT",   "OPUSDT",    "MKRUSDT",    "AAVEUSDT",
    "STXUSDT",   "VETUSDT",   "HBARUSDT",  "ALGOUSDT",   "EGLDUSDT",
    "XTZUSDT",   "AXSUSDT",   "SANDUSDT",  "MANAUSDT",   "THETAUSDT",
    "FLOWUSDT",  "KSMUSDT",   "RUNEUSDT",  "CAKEUSDT",   "COMPUSDT",
    "SUSHIUSDT", "SNXUSDT",   "YFIUSDT",   "ZECUSDT",    "DASHUSDT",
    "NEOUSDT",   "ONTUSDT",   "ZILUSDT",   "ENJUSDT",    "BATUSDT",
    "ZRXUSDT",   "STORJUSDT", "BANDUSDT",  "KNCUSDT",    "CRVUSDT",
    "1INCHUSDT", "RENUSDT",   "LRCUSDT",   "OCEANUSDT",  "CELRUSDT",
    "SKLUSDT",   "ANKRUSDT",  "CTSIUSDT",  "RLCUSDT",    "WAVESUSDT",
    "QTUMUSDT",  "ICXUSDT",   "XEMUSDT",   "NKNUSDT",    "BLZUSDT",
    "COTIUSDT",  "STMXUSDT",  "GLMUSDT",   "CKBUSDT",    "TLMUSDT",
    "MASKUSDT",  "GALAUSDT",  "IMXUSDT",   "WOOUSDT",    "GMTUSDT",
    "APEUSDT",   "LDOUSDT",   "FETUSDT",   "INJUSDT",    "SUIUSDT",
    "SEIUSDT",   "TIAUSDT",   "WLDUSDT",   "PYTHUSDT",   "JUPUSDT",
    "WIFUSDT",   "BOMEUSDT",  "ENAUSDT",   "TAOUSDT",    "NOTUSDT",
    "EIGENUSDT", "SCRUSDT",   "MOVEUSDT",  "MEUSDT",     "VIRTUALUSDT",
]


# Fetch full exchange info and index by symbol
def fetch_exchange_info():
    """
    Fetches Binance exchangeInfo and returns a dict keyed by symbol name
    for O(1) lookups. Only includes TRADING symbols with USDT as quote asset.
    """
    logging.info("Fetching exchangeInfo from Binance...")
    url = "https://api.binance.com/api/v3/exchangeInfo"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    return {
        s["symbol"]: s
        for s in resp.json()["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }


# Auto-detect listing date from klines API
def get_listing_timestamp(symbol: str):
    """
    Queries Binance for the oldest available monthly kline to find
    the real listing datetime. Returns a string in the format
    'YYYY-MM-DD HH:MM:SS' for consistency with the Gold dim_symbol table.
    Falls back to '2017-01-01 00:00:00' on any error.
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol":    symbol,
        "interval":  "1M",
        "startTime": 0,
        "limit":     1,
    }
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data:
            ts_ms = data[0][0]
            # Convert ms → datetime string
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logging.warning(f"Could not fetch listing date for {symbol}: {e}")
    return "2017-01-01 00:00:00"


# Build metadata for all valid symbols
def build_metadata(candidates: list[str], exchange_index: dict,):
    """
    1. Filters candidates against the live exchange index.
    2. Fetches listing timestamps in parallel.
    3. Returns a list of metadata dicts ready for JSON serialisation.
    """
    valid   = [s for s in candidates if s in exchange_index]
    invalid = [s for s in candidates if s not in exchange_index]

    if invalid:
        logging.warning(
            f"Dropping {len(invalid)} inactive/unlisted symbols: {invalid}"
        )
    logging.info(f"{len(valid)} symbols confirmed active on Binance.")

    # Fetch listing timestamps in parallel
    logging.info("Resolving listing timestamps in parallel...")
    timestamps: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=MAX_LISTING_WORKERS) as ex:
        futures = {ex.submit(get_listing_timestamp, sym): sym for sym in valid}
        for future in as_completed(futures):
            sym = futures[future]
            timestamps[sym] = future.result()
            logging.info(f"  {sym}: first kline at {timestamps[sym]}")

    # Compose final records
    records = []
    for sym in valid:
        s = exchange_index[sym]
        records.append({
            "symbol":            sym,
            "status":            s["status"],
            "base_asset":        s["baseAsset"],
            "base_precision":    s["baseAssetPrecision"],
            "quote_asset":       s["quoteAsset"],
            "quote_precision":   s["quotePrecision"],
            "tracking_start_ts": timestamps[sym],
        })

    records.sort(key=lambda r: r["symbol"])
    return records


# Entrypoint
def sync_filtered_metadata():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        exchange_index = fetch_exchange_info()
        records        = build_metadata(TOP_100_SYMBOLS, exchange_index)

        output = {"symbols": records}

        os.makedirs(VOLUME_PATH, exist_ok=True)
        with open(TARGET_PATH, "w") as f:
            json.dump(output, f, indent=4)

        logging.info("─" * 50)
        logging.info(f"Synced {len(records)} symbols → {TARGET_PATH}")

    except Exception as e:
        logging.error(f"Metadata sync failed: {e}")
        raise


if __name__ == "__main__":
    sync_filtered_metadata()