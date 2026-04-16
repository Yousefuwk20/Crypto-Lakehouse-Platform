import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, TimestampType,
    StringType, BooleanType, IntegerType
)

# dim_symbol
@dlt.table(
    name = "binance_platform.gold.dim_symbol",
    comment = "Curated metadata for our 10 active trading pairs",
    table_properties = {"quality": "gold"}
)

def dim_symbol():
    json_path = "/Volumes/binance_platform/default/raw_data/metadata/exchange_info.json"
    return (
        spark.read.option("multiLine", True).json(json_path)
        .select(F.explode("symbols").alias("s"))
        .select(
            "s.symbol",
            "s.status",
            "s.base_asset",
            "s.base_precision",
            "s.quote_asset",
            "s.quote_precision",
            "s.tracking_start_ts"
            )
        .withColumn("symbol_id", F.abs(F.hash("symbol")))
    )

@dlt.table(
    name    = "binance_platform.gold.dim_time",
    comment = "Time dimension — minute grain from 2019 to 2030",
    table_properties = {"quality": "gold"},
    cluster_by = ["timestamp_key"]
)

def dim_time():
    return (
        spark.sql("""
            SELECT explode(
                sequence(
                    timestamp '2019-01-01 00:00:00',
                    timestamp '2030-01-01 00:00:00',
                    interval 1 minute
                )
            ) AS timestamp_key
        """)
        .select(
            # Primary key
            "timestamp_key",

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
            F.when(F.dayofweek("timestamp_key").isin(1, 7), True)
             .otherwise(False).alias("is_weekend"),

            # Trading session 
            F.when(F.hour("timestamp_key").between(0, 7), "Asia")
             .when(F.hour("timestamp_key").between(8, 15), "Europe")
             .otherwise("US").alias("trading_session"),

            # Date only
            F.to_date("timestamp_key").alias("date"),
        )
    )


# fact_klines
@dlt.table(
    name    = "binance_platform.gold.fact_klines",
    comment = "Core fact table — 1min OHLCV per symbol",
    table_properties = {
        "quality"                         : "gold",
        "pipelines.autoOptimize.managed"  : "true"
    },
    cluster_by = ["symbol_id", "timestamp_key"]
)

@dlt.expect_or_fail("valid_symbol_key", "symbol_id IS NOT NULL")
@dlt.expect_or_fail("valid_time_key",   "timestamp_key IS NOT NULL")

def fact_klines():
    silver  = dlt.read_stream("binance_platform.silver.clean_klines")
    symbols = dlt.read("binance_platform.gold.dim_symbol")

    return (
        silver
        .join(F.broadcast(symbols), on="symbol", how="left")
        .select(
            # Foreign keys
            F.col("symbol_id"),
            F.col("open_time_ts").alias("timestamp_key"),

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

@dlt.view(
    name = "quality_gap_report",
    comment = "Identifies missing minutes in fact_klines by comparing against dim_time"
)

def quality_gap_report():
    symbols = dlt.read("binance_platform.gold.dim_symbol").select("symbol_id", "symbol", F.col("tracking_start_ts").alias("start_ts"))

    time_master = dlt.read("binance_platform.gold.dim_time").filter(
        F.col("timestamp_key") < F.expr("date_trunc('minute', current_timestamp())")
    )
    
    fact = dlt.read("binance_platform.gold.fact_klines")
    
    bounds = fact.groupBy("symbol_id")\
                 .agg(F.max("timestamp_key").alias("end_ts")
)

    expected = (
        bounds
        .join(symbols, on="symbol_id", how="inner")
        .join(
            time_master,
            (F.col("timestamp_key") >= F.col("start_ts")) &
            (F.col("timestamp_key") <= F.col("end_ts")),
            how="inner"
        )
    )

    fact_keys = fact.select("symbol_id", "timestamp_key").distinct()

    return (
        expected
        .join(fact_keys, on=["symbol_id", "timestamp_key"], how="left_anti")
        .select(
            "symbol",
            "symbol_id",
            "timestamp_key",
            F.col("trading_session"),
            F.lit("MISSING_MINUTE").alias("issue_type")
        )
    )


@dlt.table(
    name = "binance_platform.gold.agg_hourly_summary",
    comment = "Finalized hourly OHLCV and market dynamics. Emits 5 minutes after the hour closes.",
    table_properties = {"quality": "gold"},
    cluster_by = ["symbol_id", "hour_start"]
)

def agg_hourly_summary():
    return (
        dlt.read_stream("binance_platform.gold.fact_klines")
        .withWatermark("timestamp_key", "5 minutes")
        
        .groupBy(
            "symbol_id",
            F.window("timestamp_key", "1 hour").alias("time_window")
        )
        
        .agg(
            F.min_by("open", "timestamp_key").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "timestamp_key").alias("close"),
            F.sum("volume").alias("total_base_volume"),
            F.sum("quote_asset_volume").alias("total_quote_volume"),
            F.sum("number_of_trades").alias("total_trades"),
            F.sum("taker_buy_base_vol").alias("taker_buy_volume")
        )
        
        .select(
            "symbol_id",
            F.col("time_window.start").alias("hour_start"),
            
            "open", "high", "low", "close",
            "total_base_volume", 
            "total_trades",
            
            (F.col("total_quote_volume") / 
             F.when(F.col("total_base_volume") == 0, None)
             .otherwise(F.col("total_base_volume"))).alias("vwap"),

            ((F.col("high") - F.col("low")) / 
              F.when(F.col("open") == 0, None)
              .otherwise(F.col("open"))).alias("amplitude_pct"),

            (F.col("taker_buy_volume") / 
             F.when(F.col("total_base_volume") == 0, None)
             .otherwise(F.col("total_base_volume"))).alias("buy_pressure_ratio")   
        )
    )


@dlt.table(
    name = "binance_platform.gold.agg_daily_summary",
    comment = "Finalized daily OHLCV and market dynamics. Emits only when the UTC day is fully complete.",
    table_properties = {"quality": "gold"},
    cluster_by = ["symbol_id", "date"]
)

def agg_daily_summary():
    return (
        dlt.read_stream("binance_platform.gold.fact_klines")
        .withWatermark("timestamp_key", "1 hour")
        
        .groupBy(
            "symbol_id",
            F.window("timestamp_key", "1 day").alias("time_window")
        )
        
        .agg(
            F.min_by("open", "timestamp_key").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "timestamp_key").alias("close"),
            F.sum("volume").alias("total_base_volume"),
            F.sum("quote_asset_volume").alias("total_quote_volume"),
            F.sum("number_of_trades").alias("total_trades"),
            F.sum("taker_buy_base_vol").alias("taker_buy_volume")
        )
        
        .select(
            "symbol_id",
            F.to_date("time_window.start").alias("date"),
            
            "open", "high", "low", "close",
            "total_base_volume", 
            "total_trades",
                        
            (F.col("total_quote_volume") / 
             F.when(F.col("total_base_volume") == 0, None)
             .otherwise(F.col("total_base_volume"))).alias("vwap"),

            ((F.col("high") - F.col("low")) / 
              F.when(F.col("open") == 0, None)
              .otherwise(F.col("open"))).alias("amplitude_pct"),

            (F.col("taker_buy_volume") / 
             F.when(F.col("total_base_volume") == 0, None)
             .otherwise(F.col("total_base_volume"))).alias("buy_pressure_ratio")   
        )
    )