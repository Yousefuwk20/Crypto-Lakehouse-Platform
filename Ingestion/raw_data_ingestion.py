import datetime
import requests
import zipfile
import io
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
VOLUME_PATH = "/Volumes/binance_platform/default/raw_data"
BASE_PATH   = f"{VOLUME_PATH}/raw_klines"
BASE_URL    = "https://data.binance.vision/data/spot/monthly/klines"
INTERVAL    = "1m"

MAX_DOWNLOAD_WORKERS = 20
MAX_LISTING_WORKERS  = 30
REQUEST_TIMEOUT      = 30

# Top 100 coins by market cap that have active USDT spot pairs on Binance.
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


# Validate symbols against Binance exchangeInfo
def get_active_usdt_symbols():
    """
    Returns the set of currently active USDT spot symbols on Binance.
    Used to drop any symbol in TOP_100_SYMBOLS that has been delisted.
    """
    url = "https://api.binance.com/api/v3/exchangeInfo"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return {
        s["symbol"]
        for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    }


# Auto-detect listing date from Binance klines API
def get_listing_date(symbol: str):
    """
    Queries Binance for the oldest available monthly kline to find
    the true listing date. Falls back to 2017-01-01 on any error.
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
            return datetime.date.fromtimestamp(ts_ms / 1000)
    except Exception as e:
        logging.warning(f"Could not fetch listing date for {symbol}: {e}")
    return datetime.date(2017, 8, 1)


def resolve_symbols(candidates: list[str]):
    """
    1. Filters candidates against exchangeInfo (drops delisted).
    2. Fetches listing dates in parallel.
    Returns a list of dicts: {symbol, start_year, start_month}.
    """
    logging.info("Validating symbols against Binance exchangeInfo...")
    active = get_active_usdt_symbols()

    valid   = [s for s in candidates if s in active]
    invalid = [s for s in candidates if s not in active]

    if invalid:
        logging.warning(f"Skipping {len(invalid)} inactive/delisted symbols: {invalid}")
    logging.info(f"{len(valid)} active symbols confirmed.")

    logging.info("Fetching listing dates in parallel...")
    resolved = {}

    with ThreadPoolExecutor(max_workers=MAX_LISTING_WORKERS) as ex:
        futures = {ex.submit(get_listing_date, sym): sym for sym in valid}
        for future in as_completed(futures):
            sym  = futures[future]
            date = future.result()
            resolved[sym] = date
            logging.info(f"  {sym}: listed {date}")

    return [
        {
            "symbol":      sym,
            "start_year":  resolved[sym].year,
            "start_month": resolved[sym].month,
        }
        for sym in valid
    ]


# URL generation
def generate_urls(symbol: str, start_year: int, start_month: int = 1):
    """
    Generates download tasks for every month from (start_year, start_month)
    up to and including last fully-closed month.
    """
    urls = []
    now        = datetime.datetime.utcnow()
    last_valid = (now.replace(day=1) - datetime.timedelta(days=1))
    end_year   = last_valid.year
    end_month  = last_valid.month

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            # Skip months before listing
            if year == start_year and month < start_month:
                continue
            # Stop after last closed month
            if year == end_year and month > end_month:
                break
            urls.append({
                "symbol": symbol,
                "year":   year,
                "month":  month,
                "url":    (
                    f"{BASE_URL}/{symbol}/{INTERVAL}/"
                    f"{symbol}-{INTERVAL}-{year}-{month:02d}.zip"
                ),
            })
    return urls


# Download + unzip a single month
def process_month(item: dict) -> str:
    symbol = item["symbol"]
    year   = item["year"]
    month  = item["month"]

    target_dir  = f"{BASE_PATH}/symbol={symbol}/year={year}"
    target_file = f"{target_dir}/month={month:02d}.csv"

    if os.path.exists(target_file):
        return "SKIPPED"

    os.makedirs(target_dir, exist_ok=True)

    try:
        response = requests.get(item["url"], timeout=REQUEST_TIMEOUT)

        if response.status_code == 404:
            return "NOT_FOUND"
        if response.status_code != 200:
            return f"FAILED (HTTP {response.status_code})"

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as src, open(target_file, "wb") as dst:
                dst.write(src.read())

        return "SUCCESS"

    except zipfile.BadZipFile:
        return "FAILED (bad ZIP)"
    except Exception as e:
        return f"FAILED ({e})"


# Orchestration
def run_bulk_ingestion():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    # Validate + resolve listing dates
    symbols = resolve_symbols(TOP_100_SYMBOLS)

    # Build full task list
    all_tasks = []
    for cfg in symbols:
        all_tasks.extend(
            generate_urls(cfg["symbol"], cfg["start_year"], cfg["start_month"])
        )

    total = len(all_tasks)
    logging.info(f"Total download tasks: {total} across {len(symbols)} symbols")

    # Download in parallel
    results = []
    with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as executor:
        futures = {executor.submit(process_month, task): task for task in all_tasks}
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            task = futures[future]
            if result not in ("SKIPPED", "NOT_FOUND", "SUCCESS"):
                logging.error(
                    f"[{i}/{total}] {task['symbol']} "
                    f"{task['year']}-{task['month']:02d}: {result}"
                )
            elif i % 100 == 0:
                logging.info(f"Progress: {i}/{total}")

    # Summary
    success   = results.count("SUCCESS")
    skipped   = results.count("SKIPPED")
    not_found = results.count("NOT_FOUND")
    failed    = sum(1 for r in results if r.startswith("FAILED"))

    logging.info("─" * 50)
    logging.info(f"Done — {total} tasks")
    logging.info(f"Downloaded : {success}")
    logging.info(f"Skipped    : {skipped}")
    logging.info(f"Not found  : {not_found}")
    logging.info(f"Failed     : {failed}")


if __name__ == "__main__":
    run_bulk_ingestion()