import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

@dlt.view(name="staged_klines")
def staged_klines():
    return (
        spark.readStream.table("binance_platform.bronze.raw_klines")
        .withColumn("open", F.col("open").cast(DecimalType(18,8)))
        .withColumn("high", F.col("high").cast(DecimalType(18,8)))
        .withColumn("low", F.col("low").cast(DecimalType(18,8)))
        .withColumn("close", F.col("close").cast(DecimalType(18,8)))
        .withColumn("volume", F.col("volume").cast(DecimalType(18,8)))
        .withColumn("quote_asset_volume", F.col("quote_asset_volume").cast(DecimalType(18,8)))
        .withColumn("taker_buy_base_vol", F.col("taker_buy_base_vol").cast(DecimalType(18,8)))
        .withColumn("taker_buy_quote_vol", F.col("taker_buy_quote_vol").cast(DecimalType(18,8)))
        .withColumn("open_time",
            F.when(
                F.col("open_time") > 9999999999999,  # 16-digit microseconds
                (F.col("open_time") / 1000000).cast(TimestampType())
            ).otherwise(
                (F.col("open_time") / 1000).cast(TimestampType())
            )
        )
        .withColumn("close_time",
            F.when(
                F.col("close_time") > 9999999999999,
                (F.col("close_time") / 1000000).cast(TimestampType())
            ).otherwise(
                (F.col("close_time") / 1000).cast(TimestampType())
            )
        )
        .withColumn("price_range", F.col("high") - F.col("low"))
        .withColumn("price_change", F.col("close") - F.col("open"))
        .withColumn("price_change_pct", ((F.col("close") - F.col("open")) / F.col("open")) * 100)
        .withColumn("buy_sell_ratio",
            F.when(
                F.col("volume") > 0,
                F.col("taker_buy_base_vol") / F.col("volume")
            ).otherwise(F.lit(None))
        )
    )

dlt.create_streaming_table(
    name    = "clean_klines",
    comment = "Cleaned, typed, validated, deduplicated klines",
    table_properties = {
        "quality"                        : "silver",
        "pipelines.autoOptimize.managed" : "true",
        "delta.autoOptimize.optimizeWrite": "true",
    }
)

dlt.apply_changes(
    target     = "clean_klines",
    source     = "staged_klines",
    keys       = ["symbol", "open_time"],
    sequence_by = F.col("ingestion_timestamp"),
    stored_as_scd_type = 1
)