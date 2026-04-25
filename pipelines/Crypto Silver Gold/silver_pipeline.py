import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

@dlt.view(name="v_staged_klines")
def v_staged_klines():
    return (
        dlt.read_stream("binance_platform.bronze.unified_bronze_klines")
        .withColumn("open", F.col("open").cast(DecimalType(18,8)))
        .withColumn("high", F.col("high").cast(DecimalType(18,8)))
        .withColumn("low", F.col("low").cast(DecimalType(18,8)))
        .withColumn("close", F.col("close").cast(DecimalType(18,8)))
        .withColumn("volume", F.col("volume").cast(DecimalType(18,8)))
        .withColumn("quote_asset_volume", F.col("quote_asset_volume").cast(DecimalType(18,8)))
        .withColumn("taker_buy_base_vol", F.col("taker_buy_base_vol").cast(DecimalType(18,8)))
        .withColumn("taker_buy_quote_vol", F.col("taker_buy_quote_vol").cast(DecimalType(18,8)))
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
                    (F.when(F.col("open") != 0,
                            (F.col("close") - F.col("open")) / F.col("open") *100)
                     .otherwise(None))
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
    "valid_timestamp": "open_time_ts IS NOT NULL AND open_time_ts > '2017-08-01'",
    "valid_symbol": "symbol IS NOT NULL"
}

@dlt.view(name="v_valid_klines")
def v_valid_klines():
    pass_condition = " AND ".join([f"({r})" for r in KLINE_RULES.values()])
    return dlt.read_stream("v_staged_klines").filter(pass_condition)

# The Dead Letter Queue (DLQ) Table
@dlt.table(
    name = "binance_platform.silver.quarantine_klines",
    comment = "Records that failed data quality expectations",
    table_properties = {"quality": "quarantine"}
)

def quarantine_klines():
    fail_condition = "NOT (" + " AND ".join([f"({r})" for r in KLINE_RULES.values()]) + ")"
    
    return (
        dlt.read_stream("v_staged_klines")
        .filter(fail_condition)
        .withColumn("quarantine_ts", F.current_timestamp())
        .withColumn("failed_rule",
            F.when(~F.expr(KLINE_RULES["valid_price"]),     F.lit("valid_price"))
             .when(~F.expr(KLINE_RULES["valid_timestamp"]), F.lit("valid_timestamp"))
             .when(~F.expr(KLINE_RULES["valid_symbol"]),    F.lit("valid_symbol"))
             .otherwise(F.lit("multiple_or_unknown"))
        )
    )

dlt.create_streaming_table(
    name = "binance_platform.silver.clean_klines",
    table_properties = {
        "quality": "silver",
        "delta.enableDeletionVectors": "true"
        },
    cluster_by = ["symbol", "open_time"] 
)

dlt.apply_changes(
    target = "binance_platform.silver.clean_klines",
    source = "v_valid_klines",
    keys = ["symbol", "open_time_ts"],
    sequence_by = F.col("ingestion_timestamp"),
    stored_as_scd_type = 1
)
