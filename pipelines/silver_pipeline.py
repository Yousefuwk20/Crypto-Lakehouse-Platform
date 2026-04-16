import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

@dlt.view(name="staged_klines")
def staged_klines():
    return (
        dlt.read_stream("binance_platform.bronze.raw_klines")
        .select("*")
        .withColumn("open", F.col("open").cast(DecimalType(38,18)))
        .withColumn("high", F.col("high").cast(DecimalType(38,18)))
        .withColumn("low", F.col("low").cast(DecimalType(38,18)))
        .withColumn("close", F.col("close").cast(DecimalType(38,18)))
        .withColumn("volume", F.col("volume").cast(DecimalType(38,18)))
        .withColumn("quote_asset_volume", F.col("quote_asset_volume").cast(DecimalType(38,18)))
        .withColumn("taker_buy_base_vol", F.col("taker_buy_base_vol").cast(DecimalType(38,18)))
        .withColumn("taker_buy_quote_vol", F.col("taker_buy_quote_vol").cast(DecimalType(38,18)))
        .withColumn("open_time_ts",
            F.when(
                F.col("open_time") > 9999999999999,
                (F.col("open_time") / 1000000).cast(TimestampType())
            ).otherwise(
                (F.col("open_time") / 1000).cast(TimestampType())
            )
        )
        .withColumn("close_time_ts",
            F.when(
                F.col("close_time") > 9999999999999,
                (F.col("close_time") / 1000000).cast(TimestampType())
            ).otherwise(
                (F.col("close_time") / 1000).cast(TimestampType())
            )
        )
        .withColumn("price_range", F.col("high") - F.col("low"))
        .withColumn("price_change", F.col("close") - F.col("open"))
        .withColumn("price_change_pct", 
            ((F.col("close") - F.col("open")) / F.nullif(F.col("open"), F.lit(0))) * 100
        )
        .withColumn("buy_sell_ratio",
            F.when(
                F.col("volume") > 0,
                F.col("taker_buy_base_vol") / F.col("volume")
            ).otherwise(F.lit(None))
        )
    )

# Data Quality Rules
KLINE_RULES = {
    "valid_price": "close > 0 AND high >= low",
    "valid_timestamp": "open_time_ts IS NOT NULL AND open_time_ts > '2018-12-31'",
    "valid_symbol": "symbol IS NOT NULL"
}

# The Dead Letter Queue (DLQ) Table
@dlt.table(
    name = "binance_platform.silver.quarantine_klines",
    comment = "Records that failed data quality expectations",
    table_properties = {"quality": "quarantine"}
)

def quarantine_klines():
    fail_condition = "NOT (" + " AND ".join([f"({r})" for r in KLINE_RULES.values()]) + ")"
    
    return (
        dlt.read_stream("staged_klines")
        .filter(fail_condition)
        .withColumn("quarantine_ts", F.current_timestamp())
        .select("symbol", "open_time", "open_time_ts", "source_file", "quarantine_ts")
    )

dlt.create_streaming_table(
    name = "binance_platform.silver.clean_klines",
    table_properties = {
        "quality": "silver",
        "delta.enableDeletionVectors": "true",
        "delta.enableChangeDataFeed": "true" 
    },
    cluster_by = ["symbol", "open_time_ts"] 
)

dlt.apply_changes(
    target = "binance_platform.silver.clean_klines",
    source = "staged_klines",
    keys = ["symbol", "open_time_ts"],
    sequence_by = F.col("ingestion_timestamp"),
    where = " AND ".join([f"({r})" for r in KLINE_RULES.values()]),
    stored_as_scd_type = 1
)