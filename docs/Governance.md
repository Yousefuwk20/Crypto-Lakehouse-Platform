## Crypto Lakehouse Data Governance Standard

This document outlines the data governance policies, naming conventions, and best practices for the Crypto Lakehouse Platform. Following these standards ensures data quality, consistency, security, and traceability across our Medallion Architecture (Bronze, Silver, Gold).

## 1. Medallion Architecture & Naming Conventions

Our data platform implements the **Medallion Architecture**, structured into three distinct layers:

### 1.1 Bronze (Raw Layer)
- **Purpose:** Stores raw, unprocessed data ingested directly from source systems (e.g., Binance API). Schema on read.
- **Naming Standard:** `[domain].bronze.[entity]` 
- **Example:** `binance_platform.bronze.raw_klines`
- **Data Retention:** Retain historical raw data indefinitely or based on cold storage policies to allow for full reprocessing.

### 1.2 Silver (Cleansed & Conformed Layer)
- **Purpose:** Cleansed, enriched, and normalized data. This layer enforces schema validation, standardizes data types (e.g., Decimal calculations for currency), and filters out anomalies.
- **Naming Standard:** `[domain].silver.[entity]`
- **Examples:** 
  - `binance_platform.silver.clean_klines` (Valid records)
  - `staged_klines` (Processing/Transformation views)
- **Quarantine (DLQ):** Invalid records are rerouted to a quarantine table.
  - **Naming:** `[domain].silver.quarantine_[entity]`
  - **Example:** `binance_platform.silver.quarantine_klines`

### 1.3 Gold (Curated & Aggregated Layer)
- **Purpose:** Business-level aggregates, reporting views, and ready-to-use analytical tables. Organized by business value or product.
- **Naming Standard:** `[domain].gold.[type]_[entity_or_metric]` (where `[type]` is `dim`, `fact`, `agg`, `v`, etc.)
- **Examples:** `binance_platform.gold.agg_hourly_summary`, `binance_platform.gold.fact_klines`, `binance_platform.gold.dim_symbol`

## 2. Data Quality & Validation Rules (Expectations)

Data Quality is stringently enforced before data enters the Silver layer.

- **Quarantine Strategy (Dead Letter Queue):** 
  Records failing Data Quality rules must not fail the pipeline but should be isolated into a quarantine table (e.g., `slv_quarantine_klines`). This allows analysts to investigate bad data without halting ingestion.
- **Rule Definitions:** Rules should be centrally defined (e.g., `KLINE_RULES`).
  - *Example expectations:* 
    - `valid_price`: `close > 0 AND high >= low`
    - `valid_timestamp`: `open_time_ts IS NOT NULL AND open_time_ts > '2018-12-31'`
    - `valid_symbol`: `symbol IS NOT NULL`
- **SCD Management:** Silver and Gold tables should manage Slowly Changing Dimensions seamlessly (e.g., using `dlt.apply_changes` for SCD Type 1 updates based on ingestion timestamps).

## 3. Schema & Data Types Standardization

Given the domain of Cryptocurrency, precision is critical.
- **Currency & Prices:** All pricing and volume data must be explicitly cast to High-Precision Decimals (e.g., `DecimalType(18,8)`) to avoid the floating-point inaccuracies typical in `DoubleType`.
- **Timestamps:** Timestamps must be consistently standardized. Handlers must accommodate variations from source systems (e.g., milliseconds vs microseconds) and cast them to `TimestampType`.
- **Primary Keys:** Identify clear primary keys (e.g., `symbol` + `open_time_ts`) for downstream deduplication/upserts.

## 4. Performance & Storage Best Practices
- **Storage Format:** Use **Delta Lake** natively for all layers.
- **Clustering / Partitioning:** 
  - Do not use traditional partitioning for high-cardinality columns. 
  - Use Liquid Clustering on queried/joined columns like `["symbol", "open_time_ts"]`.
- **Table Properties:** 
  - Enable Deletion Vectors (`delta.enableDeletionVectors = true`) to speed up row-level updates/deletes.
  - Enable Change Data Feed (`delta.enableChangeDataFeed = true`) for downstream incremental processing.

## 5. Metadata & Lineage

### Required Metadata Fields (Bronze)

All Bronze tables **MUST** include:

```python
.withColumn("source_file", F.col("_metadata.file_path"))
.withColumn("symbol", F.regexp_extract(F.col("_metadata.file_path"), r"...", 1))
.withColumn("ingestion_timestamp", F.current_timestamp())
.withColumn("source_system", F.lit("binance_spot_rest"))
```

| Field | Type | Purpose | Example |
|-------|------|---------|---------|
| `source_file` | string | File path for audit trail | `/Volumes/binance_platform/default/.../BTCUSDT/2026/01.csv` |
| `symbol` | string | Trading pair identifier | `BTCUSDT` |
| `ingestion_timestamp` | timestamp | When data entered lakehouse | `2026-04-17 10:30:45` |
| `source_system` | string | System of record | `binance_spot_rest` |

