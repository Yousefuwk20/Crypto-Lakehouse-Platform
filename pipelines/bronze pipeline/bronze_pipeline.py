import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, IntegerType
)

BRONZE_PATH = (
    "abfss://bronze@cryptolandzone"
    ".dfs.core.windows.net/raw_klines"
)

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
    name    = "raw_klines",
    comment = "Raw kline data ingested from Binance Vision via Autoloader",
    table_properties = {
        "quality"                          : "bronze",
        "pipelines.autoOptimize.managed"   : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
    }
)

@dlt.expect("valid_open_time",       "open_time IS NOT NULL")
@dlt.expect("valid_close_time",      "close_time IS NOT NULL")
@dlt.expect("valid_open_price",      "open IS NOT NULL")
@dlt.expect("non_negative_trades", "number_of_trades >= 0")
def raw_klines():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",             "csv")
             .option("cloudFiles.schemaLocation",
                     f"{BRONZE_PATH}/_schema_hints")
             .option("cloudFiles.inferColumnTypes",   "false")
             .option("read.file_path",                "true")
             .option("header",                        "false")
             .schema(RAW_SCHEMA)
             .load(BRONZE_PATH)
             # Add metadata columns
             .withColumn("symbol", F.regexp_extract(F.col("_metadata.file_path"), r"symbol=([^/]+)",1))
             .withColumn("ingestion_timestamp", F.current_timestamp())
             .withColumn("source_system", F.lit("binance_spot_rest"))
             .withColumn("pipeline_run_id", F.expr("uuid()"))
             .drop("ignore")
    )