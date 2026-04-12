import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, TimestampType,
    StringType, BooleanType, IntegerType
)

# dim_symbol
# Static reference table — manually defined, 10 rows
@dlt.table(
    name    = "dim_symbol",
    comment = "Symbol reference dimension — manually maintained",
    table_properties = {"quality": "gold"}
)
def dim_symbol():
    data = [
        (1,  "BTCUSDT",  "BTC",  "USDT", "Layer1",  "Large", True),
        (2,  "ETHUSDT",  "ETH",  "USDT", "Layer1",  "Large", True),
        (3,  "BNBUSDT",  "BNB",  "USDT", "Exchange","Large", True),
        (4,  "SOLUSDT",  "SOL",  "USDT", "Layer1",  "Large", True),
        (5,  "ADAUSDT",  "ADA",  "USDT", "Layer1",  "Mid",   True),
        (6,  "XRPUSDT",  "XRP",  "USDT", "Payment", "Large", True),
        (7,  "DOGEUSDT", "DOGE", "USDT", "Meme",    "Large", True),
        (8,  "AVAXUSDT", "AVAX", "USDT", "Layer1",  "Mid",   True),
        (9,  "LINKUSDT", "LINK", "USDT", "DeFi",    "Mid",   True),
        (10, "DOTUSDT",  "DOT",  "USDT", "Layer0",  "Mid",   True),
    ]
    columns = [
        "symbol_id", "symbol", "base_asset",
        "quote_asset", "category",
        "market_cap_tier", "is_active"
    ]
    return spark.createDataFrame(data, columns)


# dim_time
# Generated time dimension — every minute 2019 → today
@dlt.table(
    name    = "dim_time",
    comment = "Time dimension — minute grain from 2019 to present",
    table_properties = {"quality": "gold"}
)
def dim_time():
    return (
        spark.sql("""
            SELECT explode(
                sequence(
                    timestamp '2019-01-01 00:00:00',
                    current_timestamp(),
                    interval 1 minute
                )
            ) AS timestamp_key
        """)
        .select(
            # Primary key
            F.col("timestamp_key"),

            # Date parts
            F.year("timestamp_key").alias("year"),
            F.month("timestamp_key").alias("month"),
            F.dayofmonth("timestamp_key").alias("day"),
            F.hour("timestamp_key").alias("hour"),
            F.minute("timestamp_key").alias("minute"),
            F.quarter("timestamp_key").alias("quarter"),

            # Day metadata 
            F.date_format("timestamp_key", "EEEE").alias("day_name"),
            F.date_format("timestamp_key", "EEE").alias("day_name_short"),
            F.dayofweek("timestamp_key").alias("day_of_week"),

            # Weekend flag
            F.when(
                F.dayofweek("timestamp_key").isin(1, 7),
                True
            ).otherwise(False)
             .alias("is_weekend"),

            # Trading session 
            F.when(
                F.hour("timestamp_key").between(0, 7),
                "Asia"
            ).when(
                F.hour("timestamp_key").between(8, 15),
                "Europe"
            ).otherwise("US")
             .alias("trading_session"),

            # Date only
            F.to_date("timestamp_key").alias("date"),
        )
    )


# fact_klines
@dlt.table(
    name    = "fact_klines",
    comment = "Core fact table — 1min OHLCV per symbol",
    table_properties = {
        "quality"                         : "gold",
        "pipelines.autoOptimize.managed"  : "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact"  : "true",
    },
    cluster_by = ["symbol_id", "timestamp_key"]
)
@dlt.expect_or_fail("valid_symbol_key", "symbol_id IS NOT NULL")
@dlt.expect_or_fail("valid_time_key",   "timestamp_key IS NOT NULL")
@dlt.expect_or_fail("valid_close",      "close > 0")
def fact_klines():
    silver  = spark.read.table("binance_platform.silver.clean_klines")
    symbols = dlt.read("dim_symbol")

    return (
        silver
        .join(F.broadcast(symbols), on="symbol", how="left")
        .select(
            # Foreign keys
            F.col("symbol_id"),
            F.col("open_time").alias("timestamp_key"),

            # OHLCV measures
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("quote_asset_volume"),
            F.col("number_of_trades"),
            F.col("taker_buy_base_vol"),
            F.col("taker_buy_quote_vol"),

            # Derived measures from Silver
            F.col("price_range"),
            F.col("price_change"),
            F.col("price_change_pct"),
            F.col("buy_sell_ratio"),
        )
    )


# agg_hourly_ohlcv
@dlt.table(
    name = "agg_hourly_ohlcv",
    table_properties = {
        "quality": "gold",
    },
    cluster_by = ["symbol_id", "hour_bucket"]
)

def agg_hourly_ohlcv():
    fact = dlt.read("fact_klines")
    symbols = dlt.read("dim_symbol")

    stats = (
        fact
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("timestamp_key")))
        .groupBy("symbol_id", "hour_bucket")
        .agg(
            F.min_by(F.col("open"), F.col("timestamp_key")).alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by(F.col("close"), F.col("timestamp_key")).alias("close"),

            F.sum("volume").alias("volume"),
            F.sum("quote_asset_volume").alias("quote_asset_volume"),
            F.sum("number_of_trades").alias("number_of_trades"),

            (F.max("high") - F.min("low")).alias("price_range"),
            F.avg("price_change_pct").alias("avg_price_change_pct"),
            F.avg("buy_sell_ratio").alias("avg_buy_sell_ratio")
        )
    )

    return (
        stats.join(symbols.select("symbol_id", "symbol"), on="symbol_id", how="left")
    )


# agg_daily_summary
@dlt.table(
    name = "agg_daily_summary",
    comment = "Daily summary metrics per symbol",
    table_properties = {
        "quality": "gold",
    },
    cluster_by = ["date", "symbol_id"]
)
def agg_daily_summary():
    fact = dlt.read("fact_klines")
    symbols = dlt.read("dim_symbol")

    daily_base = (
        fact
        .withColumn("date", F.to_date(F.col("timestamp_key")))
        .groupBy("symbol_id", "date")
        .agg(
            F.min_by(F.col("open"), F.col("timestamp_key")).alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by(F.col("close"), F.col("timestamp_key")).alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("number_of_trades").alias("total_trades"),
            F.sum("quote_asset_volume").alias("quote_volume"),
            F.avg("buy_sell_ratio").alias("avg_buy_sell_ratio")
        )
    )

    return (
        daily_base
        .withColumn("price_change", F.col("close") - F.col("open"))
        .withColumn("price_change_pct", (F.col("price_change") / F.col("open")) * 100)
        .withColumn("volatility_pct", ((F.col("high") - F.col("low")) / F.col("open")) * 100)
        .join(F.broadcast(symbols.select("symbol_id", "symbol")), on="symbol_id", how="left")
    )