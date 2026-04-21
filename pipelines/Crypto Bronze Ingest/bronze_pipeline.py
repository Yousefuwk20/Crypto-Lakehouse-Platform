import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, 
    StringType, IntegerType, BooleanType
)

BRONZE_VOLUME_PATH = "/Volumes/binance_platform/default/raw_data"
INGESTION_PATH    = f"{BRONZE_VOLUME_PATH}/raw_klines"

# Schemas
RAW_SCHEMA = StructType([
    StructField("open_time",           LongType(),    True),
    StructField("open",                StringType(),  True),
    StructField("high",                StringType(),  True),
    StructField("low",                 StringType(),  True),
    StructField("close",               StringType(),  True),
    StructField("volume",              StringType(),  True),
    StructField("close_time",          LongType(),    True),
    StructField("quote_asset_volume",  StringType(),  True),
    StructField("number_of_trades",    IntegerType(), True),
    StructField("taker_buy_base_vol",  StringType(),  True),
    StructField("taker_buy_quote_vol", StringType(),  True),
    StructField("ignore",              StringType(),  True),
])

@dlt.view(
    name = "v_raw_klines",
    comment = "Raw kline data ingested from UC Volume via Auto Loader",
)

def v_raw_klines():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "false")
            .schema(RAW_SCHEMA)
            .load(INGESTION_PATH)
            .withColumn("source_file", F.col("_metadata.file_path"))
            .withColumn("symbol", F.regexp_extract(F.col("_metadata.file_path"), r"symbol=([^/]+)", 1))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_system", F.lit("binance_spot_rest"))
            .withColumn("is_final", F.lit(True))
            .drop("ignore")
    )

@dlt.table(
    name    = "raw_klines_stream",
    comment = "Parsed Kafka stream — replayable Bronze source",
    table_properties = {
        "quality"                         : "bronze",
        "delta.enableDeletionVectors"     : "true"
    }
)

def raw_klines_stream():
    EH_NAMESPACE  = "binance-streaming"
    EH_NAME = "klines-raw"
    EH_CONNECTION_STRING = spark.conf.get("connection-string")
    
    jaas_config = (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required username="$ConnectionString" '
        f'password="{EH_CONNECTION_STRING}";'
    )
    
    KAFKA_OPTIONS = {
    "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe"                : EH_NAME,
    "kafka.sasl.mechanism"     : "PLAIN",
    "kafka.security.protocol"  : "SASL_SSL",
    "kafka.sasl.jaas.config"   : jaas_config,
    "kafka.request.timeout.ms" : 10000,
    "kafka.session.timeout.ms" : 10000,
    "maxOffsetsPerTrigger"     : 10000,
    "failOnDataLoss"           : 'false',
    "startingOffsets"          : 'earliest'
    }

    return (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
        .withColumn("json_str", F.col("value").cast("string"))
        .select(
            F.get_json_object("json_str", "$.s")          .alias("symbol"),
            F.get_json_object("json_str", "$.k.t").cast("long").alias("open_time"),
            F.get_json_object("json_str", "$.k.T").cast("long").alias("close_time"),
            F.get_json_object("json_str", "$.k.o")        .alias("open"),
            F.get_json_object("json_str", "$.k.h")        .alias("high"),
            F.get_json_object("json_str", "$.k.l")        .alias("low"),
            F.get_json_object("json_str", "$.k.c")        .alias("close"),
            F.get_json_object("json_str", "$.k.v")        .alias("volume"),
            F.get_json_object("json_str", "$.k.n").cast("int").alias("number_of_trades"),
            F.get_json_object("json_str", "$.k.q")        .alias("quote_asset_volume"),
            F.get_json_object("json_str", "$.k.V")        .alias("taker_buy_base_vol"),
            F.get_json_object("json_str", "$.k.Q")        .alias("taker_buy_quote_vol"),
            F.get_json_object("json_str", "$.k.x").cast("boolean").alias("is_final"),
            F.current_timestamp()                          .alias("ingestion_timestamp"),
            F.lit("binance_spot_websocket")                .alias("source_system"),
            F.lit("event_hub_stream")                      .alias("source_file"),
        )
    )

@dlt.table(
    name    = "unified_bronze_klines",
    comment = "Single Bronze table — CSV batch + parsed Kafka stream",
    table_properties = {
        "quality"                         : "bronze",
        "delta.enableDeletionVectors"     : "true"
    }
)

def unified_bronze_klines():
    stream_df = dlt.read_stream("raw_klines_stream")   
    batch_df  = dlt.read_stream("v_raw_klines")
    return stream_df.unionByName(batch_df, allowMissingColumns=True)
