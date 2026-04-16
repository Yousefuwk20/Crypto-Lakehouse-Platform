import requests
import json
import os
import logging

VOLUME_PATH = "/Volumes/binance_platform/default/raw_data/metadata"
FILE_NAME   = "exchange_info.json"
TARGET_PATH = f"{VOLUME_PATH}/{FILE_NAME}"


COIN_CONFIG = {
    "BTCUSDT":  {"start_ts": "2019-01-01 00:00:00"},
    "ETHUSDT":  {"start_ts": "2019-01-01 00:00:00"},
    "BNBUSDT":  {"start_ts": "2019-01-01 00:00:00"},
    "XRPUSDT":  {"start_ts": "2019-01-01 00:00:00"},
    "DOGEUSDT": {"start_ts": "2019-07-05 12:00:00"},
    "SOLUSDT":  {"start_ts": "2020-08-11 06:00:00"},
    "AVAXUSDT": {"start_ts": "2020-09-22 06:30:00"},
    "ADAUSDT":  {"start_ts": "2019-01-01 00:00:00"},
    "LINKUSDT": {"start_ts": "2019-01-16 10:00:00"},
    "DOTUSDT":  {"start_ts": "2020-08-18 23:00:00"}
}

def sync_filtered_metadata():
    logging.basicConfig(level=logging.INFO)
    url = "https://api.binance.com/api/v3/exchangeInfo"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        full_data = response.json()
        
        filtered_symbols = []
        for s in full_data['symbols']:
            symbol_name = s['symbol']
            
            if symbol_name in COIN_CONFIG:
                filtered_symbols.append({
                    "symbol": symbol_name,
                    "status": s['status'],
                    "base_asset": s['baseAsset'],
                    "base_precision": s['baseAssetPrecision'],
                    "quote_asset": s['quoteAsset'],
                    "quote_precision": s['quotePrecision'],
                    "tracking_start_ts": COIN_CONFIG[symbol_name]["start_ts"]
                })
                
        output_data = {"symbols": filtered_symbols}
        
        os.makedirs(VOLUME_PATH, exist_ok=True)
        with open(TARGET_PATH, "w") as f:
            json.dump(output_data, f, indent=4)
            
        logging.info(f"Synced {len(filtered_symbols)} symbols with custom tracking dates.")
        
    except Exception as e:
        logging.error(f"Failed to sync metadata: {e}")
        raise e

if __name__ == "__main__":
    sync_filtered_metadata()