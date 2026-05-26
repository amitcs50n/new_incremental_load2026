# Pharma Sales Incremental Data Pipeline

End-to-end incremental ETL pipeline moving pharmaceutical sales data from MySQL to a Snowflake star schema. Built with Azure Data Factory, ADLS Gen2, Databricks (PySpark), and Snowflake. Production-grade features include two-phase commit watermarks, SCD Type 2 history tracking, and structured logging from Databricks with run_id propagated from ADF for cross-system traceability.

```
MySQL (OLTP source)
   ↓ [ADF Copy: metadata-driven, watermark-based incremental]
ADLS Gen2 (partitioned by year/month/day, parquet)
   ↓ [Databricks: PySpark transformations + 5 MERGE templates]
Snowflake (star schema: RAW staging → CORE dim+fact + METADATA logs)
```

**One ADF trigger → 7 million rows refreshed in ~3 minutes, with full audit trail.**

---
- **Two-phase commit** for watermark advancement (pending → live only on downstream success)
- **Race-safe watermark capture** (upper bound captured before copy, copy bounded to it — no silent data loss)
- **SCD Type 2** on customer dimension with hash-based change detection
- **Late-arriving dimension handling** in FACT MERGE (defensive backfill via second WHEN MATCHED)
- **Structured logging** from Databricks with cross-system traceability via ADF run_id
- **Type-cleansing PySpark transformations** with row-count assertions
- **Control plane / data plane separation** (watermarks in Azure SQL; analytics + audit logs in Snowflake)

The project surfaced several real bugs during development that I now talk about in interviews — including a watermark race condition that could have caused silent data loss. All are documented below.

---

## Architecture

### Data flow

| Stage | Component | What happens |
|---|---|---|
| 1. Source | MySQL `pharma` DB | 5 tables: territories (100), products (5K), sales_reps (800), customers (80.5K), sales_transactions (6M). Every table has `created_at`, `updated_at`, `is_deleted` for incremental support. |
| 2. Ingestion | ADF metadata-driven ForEach | Reads `pipeline_watermark_audit` config (Azure SQL), loops over 5 tables, Copy activity uses watermark-based incremental SELECT, writes parquet to ADLS partitioned by `year=/month=/day=`. |
| 3. Two-phase commit | ADF Script activities | Stages new watermark to `pipeline_watermark_pending` per table. Promoted to `pipeline_watermark_audit` only after Databricks success. Cleaned up on failure. |
| 4. Transformation + load | Databricks notebook | Reads all parquet partitions, applies PySpark transformations (type casts, derived columns, row-count assertion), writes to Snowflake STG tables. |
| 5. MERGE | Databricks → Snowflake | Three template patterns: SCD1 for static dims, SCD2 for customer history, FACT MERGE with 4-way dim join + late-arriving dim handling. |
| 6. Audit | Snowflake `METADATA.pipeline_run_log` | Every Databricks activity writes start/end rows tagged with the same `run_id` that ADF generated. Two-level granularity: DISPATCHER + per-template. ADF's own activity runs are observable via the ADF Monitor tab. |

### Control plane vs data plane separation

```
┌──────────────────────────────────────┐
│  CONTROL PLANE — Azure SQL           │
│  pipeline_watermark_audit            │
│  pipeline_watermark_pending          │
│  Always available, cheap, fast       │
└──────────────────────────────────────┘
              │
              │ orchestrates
              ▼
┌──────────────────────────────────────┐
│  DATA PLANE — Snowflake              │
│  RAW.STG_* (overwritten each run)    │
│  CORE.DIM_*, FACT_* (persistent)     │
│  METADATA.pipeline_run_log (audit)   │
└──────────────────────────────────────┘
```

Watermarks live in Azure SQL because they need to be available even if Snowflake has issues — orchestration cannot depend on the data plane being healthy. Analytics + audit logs live in Snowflake because that's where the analytical query patterns are.

---

## Real performance numbers (from logged runs)

| Activity | Source rows | Typical duration |
|---|---|---|
| DISPATCHER (whole pipeline) | — | ~160-210s |
| merge_scd1 — territories | 100 | 15s cold, ~5s warm |
| merge_scd1 — products | 5,000 | 8-12s |
| merge_scd1 — sales_reps | 800 | 8-9s |
| merge_scd2_customers | 80,500 | 13-14s |
| merge_fact_sales (6M-row MERGE + PySpark transforms) | 6,000,000 | 109-116s |

The first activity in each run pays a ~5-7s warehouse cold-start tax; subsequent ones run faster.

---

## Tech stack

| Layer | Technology | Version |
|---|---|---|
| Source | MySQL | 8.x (InnoDB) |
| Orchestration | Azure Data Factory v2 | — |
| Storage | Azure Data Lake Storage Gen2 | — |
| Compute | Databricks | DBR 17.3 LTS / Spark 4.0 |
| Warehouse | Snowflake | Standard edition, XSMALL warehouse |
| Control plane | Azure SQL Database | — |
| Secrets | Azure Key Vault | via Databricks secret scope |
| Languages | Python, SQL | Python 3.11 in Databricks |
| Version control | Git (this repo) | — |

---

## Bug stories (the real interview material)

### Bug 1: SCD2 staging dedup

**Symptom**: 500 customers ended up with two `is_current=TRUE` rows in DIM_CUSTOMER after pipeline ran.

**Root cause**: `read_all_parquet_from_adls` wildcards across all date partitions (`year=*/month=*/day=*`). When a customer was updated, both old and new versions existed in different partitions. The SCD2 MERGE saw the same customer_id appear twice in staging and inserted both as new versions, since neither had a current row to close.

**Fix**: Added a Step 0 dedup before MERGE — `QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1`. Keep only the latest record per natural key.

**Lesson**: When the read pattern is "read everything and let downstream sort it out," the downstream MUST sort it out. Otherwise duplicates propagate silently.

### Bug 2: FACT late-arriving dimension

**Symptom**: After the first end-to-end run, 5 million FACT rows had NULL `customer_sk`.

**Root cause**: FACT loaded before SCD2_customers completed. The LEFT JOIN against DIM_CUSTOMER returned NULL because the customer rows hadn't been inserted yet. The MERGE INSERT then committed those rows with NULL surrogate keys.

**Fix**: Two parts.
1. **Dispatcher ordering**: SCD1 dims → SCD2 dims → FACT (FACT is always last).
2. **Defensive backfill in FACT MERGE**: Added a second `WHEN MATCHED AND t.customer_sk IS NULL AND src.customer_sk IS NOT NULL` clause. Next pipeline run automatically backfills NULL surrogate keys when the dim row has now appeared.

**Lesson**: Late-arriving dimensions are a textbook DW concept. Mine wasn't from external lateness; it was from race conditions within my own pipeline. Both kinds need handling.

### Bug 3: Parquet decimal precision inflation

**Symptom**: `unit_price` in MySQL is `decimal(10,2)`. After landing in ADLS as parquet and reading back, it became `decimal(38,18)`. Same actual value, hugely inflated storage.

**Root cause**: Spark's parquet writer expanded decimal precision when it couldn't confirm bounds from upstream schema metadata.

**Fix**: Explicit cast in `apply_fact_transformations`: `.withColumn("unit_price", F.col("unit_price").cast(DecimalType(10, 2)))`. Restored to the source-defined precision before staging.

**Lesson**: Don't trust intermediate format type inference. Always cast back to the contract you expect.

### Bug 4: Watermark race condition

**Symptom**: Potential silent data loss. In the original design, the activity that computed the new watermark ran AFTER the copy completed.

**Root cause**: The copy ran `SELECT * WHERE updated_at > last_watermark` (no upper bound), then a separate step computed the max watermark. Any rows that arrived in MySQL between the copy finishing and the watermark being computed would have their `updated_at` captured into the watermark — but those rows were never copied. On the next run, the pipeline would start from `> new_watermark` and skip them forever. Silent loss, hard to detect.

**Fix**: Restructured the ForEach loop to capture the upper-bound watermark from the source BEFORE the copy (`GetUpperWatermarkBeforeCopy`), then bound the copy explicitly with `WHERE updated_at > last_watermark AND updated_at <= captured_upper_watermark`. The staged watermark is the same pre-copy value. Now the watermark always equals exactly what was copied; rows arriving after the captured ceiling are caught on the next run, not lost.

**Lesson**: In incremental pipelines, the watermark you advance must equal the upper bound of what you actually moved. Computing the watermark from anything that happens after the copy opens a race window. Capture the ceiling first, bound the copy to it, advance the watermark to it.

**Remaining edge case**: Same-second timestamp collisions straddling the copy window are still theoretically possible. Mitigated by `>` on the lower bound; fully solvable with microsecond precision or a primary-key tiebreaker. Sufficient for current volumes.

---

## Project structure

```
.
├── credential/, dataset/, factory/,
│   integrationRuntime/, linkedService/,
│   pipeline/                         ← ADF artifacts (exported from Git mode)
│
├── databricks/
│   └── pharma_sales_pipeline.ipynb   ← Main notebook (setup + 5 MERGE templates + dispatcher)
│
├── sql/
│   ├── mysql/
│   │   └── 01_create_mysql_schema.sql
│   └── snowflake/
│       ├── 01_create_snowflake_schema.sql
│       └── 03_create_log_table.sql
│
├── README.md                         ← This file
└── publish_config.json
```

---
.

---

## What I'd do next (honest limitations)

A few items I deliberately didn't ship for portfolio scope:

1. **rows_inserted / rows_updated in log table are NULL**. The Snowflake Python connector exposes `cur.rowcount` per statement, but my `execute_snowflake_sql` helper discards it. ~30 min refactor to capture and propagate.

2. **No least-privilege Snowflake role**. Currently uses ACCOUNTADMIN for all pipeline operations. Production should use a custom role with USAGE on warehouse + ALTER/INSERT/UPDATE on specific tables only.

3. **No hard-delete detection**. Watermark-based incremental can detect inserts and updates (any row with `updated_at > watermark`), but not deletions of rows whose `updated_at` never advanced. Production solution is either: (a) CDC via Debezium, (b) source-side trigger preventing hard deletes, (c) weekly reconciliation job.

4. **No ADF-side rows in `pipeline_run_log`**. Currently only Databricks activities write to the table; ADF's own activity runs (Copy, ForEach, Script) are observable via the ADF Monitor tab but aren't joined into the same log table. To unify, I'd add ADF Snowflake Script activities at pipeline start, after each Copy iteration, and at pipeline end. Roughly 1-2 hours of ADF work; deferred because the high-value compute-side logs (Databricks) are already captured.

5. **Boundary timestamp collision (edge case)**. The watermark uses strict `>` on the lower bound and `<=` on the captured upper bound. A theoretical gap remains if two rows share the exact same `updated_at` second straddling the copy window. For high-throughput systems I'd use microsecond-precision timestamps or pair the watermark with the primary key as a tiebreaker (`watermark > last OR (watermark = last AND pk > last_pk)`). Second-precision is sufficient for current volumes.

6. **SQL injection vector in logging helpers**. F-string interpolation in `log_failure` error messages is escaped, but `execute_snowflake_sql` doesn't support parameterized queries. Acceptable for this project (all values from controlled sources), would refactor to use cursor parameter binding in production.


**Built by [Amit Yadav](https://github.com/amitcs50n) — Data Engineer focused on Azure + Snowflake + Python production pipelines.**
