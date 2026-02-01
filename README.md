# training-pyspark-local — PySpark Training (Local VS Code)

Run a small PySpark project locally in VS Code: read CSVs, clean/quarantine, build KPIs, and join outputs.  
This repo is **local-first**, but the bottom section shows how to explain/port it to **Azure (ADLS + ADF + Databricks)**.

---

## Prereqs
- **Python 3.10+**
- **Java** (required by Spark)

Verify:
```bash
python --version
java -version
```

---

## Setup + Run

### 1) Create and activate a virtual environment

**Windows (PowerShell)**
```powershell
# Create venv
python -m venv .venv

# Activate venv
.\.venv\Scripts\Activate.ps1

# Install deps + install this repo as a package (src/ layout)
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

**WSL/Linux/macOS**
```bash
# Optional: reset venv
rm -rf .venv

# Create + activate venv
python3 -m venv .venv
source .venv/bin/activate

# Install deps + install this repo as a package (src/ layout)
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

---

### 2) Run scripts (module-run from repo root)

**Pattern**
```bash
python -m scripts.<FOLDER>.<FILE>
```

**Examples (Foundations)**
```bash
python -m scripts.01_foundations.01_smoke_test
python -m scripts.01_foundations.02_basics_df
python -m scripts.01_foundations.03_cleaning
python -m scripts.01_foundations.04_aggregations
python -m scripts.01_foundations.05_joins
```

---

### 3) VS Code interpreter (important)

If VS Code shows import warnings (ex: `pyspark.sql could not be resolved`), it’s almost always using the wrong Python.

1. `Ctrl+Shift+P` → **Python: Select Interpreter**
2. Select the repo venv:
   - **Windows:** `<repo>\.venv\Scripts\python.exe`
   - **WSL/Linux/macOS:** `<repo>/.venv/bin/python`
3. Reload if needed: `Ctrl+Shift+P` → **Developer: Reload Window**


---


## Project layout

> Spark writes outputs as **folders** (that’s normal).

```text
training-pyspark-local/
├─ data/
│  ├─ raw/                         # input CSVs
│  └─ out/                         # Spark outputs (folders)
|
├─ scripts/
│  ├─ 01_foundations/
│  │  ├─ __init__.py
│  │  ├─ 01_smoke_test.py
│  │  ├─ 02_basics_df.py
│  │  ├─ 03_cleaning.py
│  │  ├─ 04_aggregations.py
│  │  └─ 05_joins.py
|  |
│  ├─ 02_exercises/
│  │  ├─ __init__.py
│  │  ├─ 01_exercises.py
│  │  └─ 02_exercises_advanced.py
|  |
│  ├─ 03_engineering_patterns/
│  │  ├─ __init__.py
│  │  ├─ 01_schema_enforcement.py
│  │  ├─ 02_json_parsing.py
│  │  ├─ 03_arrays_explode.py
│  │  ├─ 04_partitioned_writes.py
│  │  ├─ 05_broadcast_cache_explain_rdd.py
│  │  └─ 06_exercises_engineering_patterns.py
|  |
│  ├─ 04_performance_debugging/
│  │  ├─ __init__.py
│  │  ├─ 01_explain_and_shuffles.py
│  │  ├─ 02_broadcast_aqe_skew_salting.py
│  │  ├─ 03_cache_persist_checkpoint.py
│  │  ├─ 04_partition_pruning_small_files.py
│  │  ├─ 05_debugging_playbook.py
│  │  └─ 06_exercises_performance_debugging.py
|  |
│  ├─ 05_incremental_delta_quality/
│  │  ├─ __init__.py
│  │  ├─ 01_delta_smoke_test.py
│  │  ├─ 02_incremental_watermarks.py
│  │  ├─ 03_quality_gates_quarantine_reasons.py
│  │  ├─ 04_delta_merge_upsert.py
│  │  ├─ 05_incremental_gold_kpis.py
│  │  └─ 06_exercises_incremental_delta_quality.py
|  |
│  └─ 06_spark_sql_advanced/
│     ├─ __init__.py
│     ├─ 01_temp_views_sql_basics.py
│     ├─ 02_ctes.py
│     ├─ 03_windows_dedupe.py
│     ├─ 04_cohorts.py
│     ├─ 05_sql_perf_hints.py
│     └─ 06_exercises_sql_views.py
|  
├─ src/
│  └─ spark_utils.py
|  
├─ requirements.txt
└─ README.md
```

### Scripts folder conventions
- Each module is a folder: `01_...`, `02_...`, etc.
- Inside each module:
  - `01–05` = foundational scripts
  - `06_*exercises*.py` = “capstone” exercises for that module (when applicable)

---

## Sample data

Create these files:

`data/raw/customers.csv`
```csv
customer_id,first_name,last_name,state,signup_date
1,Frank,Runfola,NY,2025-01-10
2,Ana,Lopez,NC,2025-02-03
3,Sam,Kim,CA,2025-02-15
4,,Patel,TX,2025-03-01
5,Jen,Chen,CA,2025-03-05
```

`data/raw/transactions.csv`
```csv
txn_id,customer_id,txn_ts,amount,merchant
1001,1,2025-03-01 10:12:00,25.10,CoffeeCo
1002,1,2025-03-02 09:01:00,120.00,ElectroMart
1003,2,2025-03-05 12:30:00,0.00,GroceryTown
1004,2,2025-03-06 17:40:00,-5.00,GroceryTown
1005,3,2025-03-07 08:00:00,18.75,CoffeeCo
1006,99,2025-03-07 09:15:00,42.00,UnknownShop
```

---

## Run

Run from the repo root using module mode (no `.py` on the module name).

✅ Requirements for module runs:
- `scripts/__init__.py` exists
- every module folder has `__init__.py`

### 01 — Foundations (start here)
```bash
python -m scripts.01_foundations.01_smoke_test
python -m scripts.01_foundations.02_basics_df
python -m scripts.01_foundations.03_cleaning
python -m scripts.01_foundations.04_aggregations
python -m scripts.01_foundations.05_joins
```

### 02 — Exercises
```bash
python -m scripts.02_exercises.01_exercises
python -m scripts.02_exercises.02_exercises_advanced
```

### 03–06 — Advanced modules
```bash
python -m scripts.03_engineering_patterns.06_exercises_engineering_patterns
python -m scripts.04_performance_debugging.06_exercises_performance_debugging
python -m scripts.05_incremental_delta_quality.06_exercises_incremental_delta_quality
python -m scripts.06_spark_sql_advanced.06_exercises_sql_views
```

Outputs land in `data/out/`.

---

## What you learn

### 01_foundations
- **01 — Smoke test**: Spark runs locally + create a DataFrame
- **02 — DataFrame basics**: read CSVs + select/filter + derived columns
- **03 — Cleaning + quarantine**: normalize + quarantine vs clean outputs
- **04 — Aggregations**: customer spend KPIs + ranking
- **05 — Joins**: join facts + dims + KPI table

### 02_exercises
- **01 — Exercises**: implement TODOs (quality rules + KPIs + writes)
- **02 — Advanced exercises**: windows + dedupe + anti-joins + pivots + cohorts

### 03_engineering_patterns
- Explicit schema + type safety
- JSON parsing + nested fields
- Arrays + explode patterns
- Partitioned writes
- Broadcast joins + cache/persist + `explain()` + a quick peek at RDDs

### 04_performance_debugging
- Shuffle vs broadcast (and when each wins)
- AQE, skew, and salting patterns
- Caching vs persisting vs checkpointing
- Partition pruning + small file avoidance
- A practical debugging playbook (what to check first)

### 05_incremental_delta_quality
- Watermark-based incremental loads
- Quarantine with **reason codes** (the real-world way)
- FK validation patterns
- Delta MERGE upserts (idempotent)
- Incremental Gold KPIs

### 06_spark_sql_advanced
- Temp views + Spark SQL muscle memory
- CTEs and readable SQL transforms
- Window functions for dedupe and “last known state”
- Cohorts + retention-style queries
- SQL performance hints and gotchas

---

## Exercises checklist

If your exercises file has TODOs, implement these staples:
- Normalize names and create `full_name`
- Quarantine txns where `amount <= 0` or `customer_id` is missing/not found
- Build KPIs: `txn_count`, `total_spend`, `last_txn_ts`
- Write outputs to `data/out/`
- Validate counts (rows in/out, quarantine %, null checks)

---

## Common issues
- **Java missing**: `java -version` must work or Spark will not start
- **Wrong interpreter in VS Code**: select `.venv`
- **Running from wrong folder**: run commands from repo root
- **Outputs are folders**: Spark writes directories, not single files

---

## Delta Lake (local pip PySpark)

Some scripts in `05_incremental_delta_quality` use **Delta MERGE**. Plain pyspark alone can’t do MERGE.

### Install compatible versions (pick one)

**Spark 3.5.x (common & stable)**
```bash
pip install "pyspark==3.5.3" "delta-spark==3.3.1"
```

**Spark 4.0.x (newest line)**
```bash
pip install "pyspark==4.0.0" "delta-spark==4.0.1"
```

Check your Spark version:
```bash
python -c "import pyspark; print(pyspark.__version__)"
```

### Enable Delta in SparkSession
Update `src/spark_utils.py` so Spark can load Delta:

```python
import pyspark
from delta.pip_utils import configure_spark_with_delta_pip

def get_spark(app_name: str):
    builder = (
        pyspark.sql.SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
```

Quick smoke test:
```python
from delta.tables import DeltaTable
spark.range(1).write.format("delta").mode("overwrite").save("data/out/_delta_smoke_test")
```

---

## Testing (this is what juniors skip)

A lot of DE teams expect at least basic **unit tests** for transforms + data quality logic.

### Install test deps
```bash
pip install pytest
```

### Run tests
From repo root:
```bash
pytest -q
```

---

## Azure-ize (how to talk about this like a real DE project)

This repo is intentionally local-first, but you should be able to explain how it becomes an Azure pipeline.

### Storage layout (ADLS Gen2)
Use a clean lake layout:
- `abfss://<container>@<account>.dfs.core.windows.net/bronze/customers/ingest_date=YYYY-MM-DD/`
- `abfss://<container>@<account>.dfs.core.windows.net/bronze/txns/ingest_date=YYYY-MM-DD/`
- `abfss://<container>@<account>.dfs.core.windows.net/silver/customers/`
- `abfss://<container>@<account>.dfs.core.windows.net/silver/txns/` *(Delta)*
- `abfss://<container>@<account>.dfs.core.windows.net/gold/daily_kpis/` *(Delta)*

### Orchestration (ADF)
Typical ADF flow:
1. Copy activity pulls raw files into **Bronze** (partitioned by `ingest_date`)
2. Databricks job runs transforms:
   - Bronze → Silver (clean + quarantine + FK checks)
   - Silver MERGE into Delta tables (idempotent upserts)
   - Gold aggregates updated incrementally
3. Logging to a run log table (`run_id`, watermark before/after, rows in/out, quarantine counts)

### Security (Managed Identity)
- ADF + Databricks authenticate to ADLS using **Managed Identity**
- Grant ADLS permissions using **RBAC** (Storage Blob Data Contributor/Reader as appropriate)
- No secrets in code, no connection strings in scripts

### Backfills + retries (production mindset)
- Parameterize runs by `run_date` (or date range)
- Backfill by reprocessing a partition range
- Retries are safe because Silver/Gold are updated with **MERGE** (idempotent)

### Cost basics (what interviewers like to hear)
- Use job clusters with autoscaling and termination
- Prefer Delta + partition pruning to reduce scan costs
- Avoid small files (coalesce/repartition before writes)
- Use broadcast joins for small dimensions; watch skew on big keys

