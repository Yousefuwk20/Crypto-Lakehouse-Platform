import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType
)

BRONZE_VOLUME_PATH = "/Volumes/binance_platform/default/raw_data"
INGESTION_PATH   = f"{BRONZE_VOLUME_PATH}/raw_klines"
CHECKPOINT_PATH  = f"{BRONZE_VOLUME_PATH}/_checkpoints/raw_klines"

# Raw schema
RAW_SCHEMA = StructType([
    StructField("open_time",             LongType(),    True),
    StructField("open",                  StringType(),  True),
    StructField("high",                  StringType(),  True),
    StructField("low",                   StringType(),  True),
    StructField("close",                 StringType(),  True),
    StructField("volume",                StringType(),  True),
    StructField("close_time",            LongType(),    True),
    StructField("quote_asset_volume",    StringType(),  True),
    StructField("number_of_trades",      IntegerType(), True),
    StructField("taker_buy_base_vol",    StringType(),  True),
    StructField("taker_buy_quote_vol",   StringType(),  True),
    StructField("ignore",                StringType(),  True),
])

# Bronze table definition
@dlt.table(
    name = "binance_platform.bronze.raw_klines",
    comment = "Raw kline data ingested from UC Volume via Auto Loader",
    table_properties = {
        "quality": "bronze",
        "delta.enableDeletionVectors": "true"
    }
)

def raw_klines():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
            .option("header", "false")
            .schema(RAW_SCHEMA)
            .load(INGESTION_PATH)
            # Metadata & Lineage
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("symbol", F.regexp_extract(F.col("_metadata.file_path"), r"symbol=([^/]+)", 1))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_system", F.lit("binance_spot_rest"))
            .drop("ignore")
    )