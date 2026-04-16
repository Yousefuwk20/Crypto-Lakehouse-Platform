import datetime
import requests
import zipfile
import io
import os
import logging
from concurrent.futures import ThreadPoolExecutor

# Configuration
VOLUME_PATH = "/Volumes/binance_platform/default/raw_data"
BASE_PATH   = f"{VOLUME_PATH}/raw_klines"
BASE_URL    = "https://data.binance.vision/data/spot/monthly/klines"
INTERVAL    = "1m"

SYMBOLS = [
    {"symbol": "BTCUSDT",  "start_year": 2019},
    {"symbol": "ETHUSDT",  "start_year": 2019},
    {"symbol": "BNBUSDT",  "start_year": 2019},
    {"symbol": "SOLUSDT",  "start_year": 2019},
    {"symbol": "ADAUSDT",  "start_year": 2019},
    {"symbol": "XRPUSDT",  "start_year": 2019},
    {"symbol": "DOGEUSDT", "start_year": 2019},
    {"symbol": "AVAXUSDT", "start_year": 2019},
    {"symbol": "LINKUSDT", "start_year": 2019},
    {"symbol": "DOTUSDT",  "start_year": 2019},
]

def generate_urls(symbol: str, start_year: int) -> list[dict]:
    urls = []
    now = datetime.datetime.utcnow()
    last_valid = now.replace(day=1) - datetime.timedelta(days=1)

    for year in range(start_year, last_valid.year + 1):
        for month in range(1, 13):
            if year == last_valid.year and month > last_valid.month:
                break
            urls.append({
                "symbol": symbol,
                "year": year,
                "month": month,
                "url": f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{year}-{month:02d}.zip"
            })
    return urls

def process_month(item):
    """Downloads, unzips, and writes to Volume."""
    target_dir = f"{BASE_PATH}/symbol={item['symbol']}/year={item['year']}"
    target_file = f"{target_dir}/month={item['month']:02d}.csv"

    if os.path.exists(target_file):
        return "SKIPPED"

    os.makedirs(target_dir, exist_ok=True)

    try:
        response = requests.get(item["url"], timeout=30)
        if response.status_code != 200:
            return f"FAILED (HTTP {response.status_code})"
        
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as source, open(target_file, "wb") as target:
                target.write(source.read())
        
        return "SUCCESS"
    except Exception as e:
        return f"FAILED ({str(e)})"

def run_bulk_ingestion():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    
    all_tasks = []
    for sym_config in SYMBOLS:
        all_tasks.extend(generate_urls(sym_config["symbol"], sym_config["start_year"]))

    logging.info(f"Starting ingestion for {len(all_tasks)} files...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_month, all_tasks))

    logging.info(f"Complete: {results.count('SUCCESS')} new, {results.count('SKIPPED')} skipped")

if __name__ == "__main__":
    run_bulk_ingestion()