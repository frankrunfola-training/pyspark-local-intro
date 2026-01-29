# pyspark-local-intro — PySpark Training (Local VS Code)

Run a small PySpark mini-project locally in VS Code: read CSVs, clean/quarantine, aggregate KPIs, and join outputs.

## Prereqs
- Python 3.10+
- Java (required by Spark)

Verify:
```bash
python --version
java -version
```

## Setup

### 1) Create and activate a virtual environment

**Windows (PowerShell)**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

**WSL/Linux/macOS**
```bash
rm -rf .venv                            # Delete the existing .venv
python3 -m venv .venv                   # Create the venv inside WSL
source .venv/bin/activate               # Activate the venv (WSL)
which python                            #Check Python:
python --version
python3 -m pip install --upgrade pip    # Install deps inside the venv
pip install -r requirements.txt
```

### 2) VS Code interpreter
- `Ctrl+Shift+P` → Python: Select Interpreter
- Select `.venv`


## Project layout
```text
pyspark-local-intro/
├─ data/
│  ├─ raw/                  # input CSVs
│  └─ out/                  # Spark outputs (folders)
├─ scripts/
│  ├─ 00_smoke_test.py
│  ├─ 01_basics_df.py
│  ├─ 02_cleaning.py
│  ├─ 03_aggregations.py
│  ├─ 04_joins.py
│  └─ 05_exercises.py
├─ src/
│  └─ spark_utils.py
├─ requirements.txt
└─ README.md
```

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

## Run

Run from repo root. Use module mode (no `.py` on the module name). Ensure `scripts/__init__.py` and `src/__init__.py` exist.

```bash
python -m scripts.00_smoke_test
python -m scripts.01_basics_df
python -m scripts.02_cleaning
python -m scripts.03_aggregations
python -m scripts.04_joins
python -m scripts.05_exercises
```

Outputs land in `data/out/` (Spark writes folders; that’s normal).

## What you learn
- **00 — Smoke test**            : Spark runs locally + create a DataFrame
- **01 — DataFrame basics**      : read CSVs + select/filter + derived columns
- **02 — Cleaning + quarantine** : normalize + quarantine vs clean outputs
- **03 — Aggregations**          : customer spend KPIs + ranking 
- **04 — Joins**                 : join fact + dimension + KPI table
- **05 — Exercises**             : implement TODOs (quality rules + KPIs + writes)

## Exercises (05_exercises.py)
Fill in the TODOs:
- normalize names and create `full_name`
- quarantine txns where `amount <= 0` or `customer_id` is missing/not found
- build KPIs: `txn_count`, `total_spend`, `last_txn_ts`
- write outputs to `data/out/`

## Common issues
- Java missing: `java -version` must work
- Wrong interpreter in VS Code: select `.venv`
- Running from wrong folder: run commands from repo root
- Spark outputs folders, not single files

## Git (optional)
```text
cd ~/projects/pyspark-local-intro
git init
git add .
git commit -m "Initial PySpark local training project"
git branch -M main
git remote add origin https://github.com/frankrunfola-training/pyspark-local-intro.git
git push -u origin main
```

---

## Advanced scripts (extra DE interview prep)

These scripts push beyond basic DataFrame transforms into topics hiring managers actually probe: windows, pivots, cohorts, performance, SQL fluency, incremental loads, logging, and data quality reporting.

### New scripts
- **06 — Advanced exercises**                : windows + dedupe + anti-joins + pivots + cohorts
- **07 — Engineering patterns**              : explicit schema, JSON parsing, explode arrays, broadcast join, partitioned writes, cache/persist, basic RDDs
- **08 — Performance + debugging**           : explain plans, shuffle vs broadcast, skew + salting, pruning, small files, checkpointing
- **09 — Incremental + Delta + DQ**          : watermark incremental loads, quarantine reasons, FK checks, Delta MERGE (Silver), incremental Gold, run logs + DQ report
- **10 — Spark SQL drills**                  : CTEs, CASE, windows, anti-joins, dedupe, cohorts, DQ counts (pure SQL)

### Run (same pattern as before)
```bash
python -m scripts.06_exercises_advanced
python -m scripts.07_exercises_engineering_patterns
python -m scripts.08_exercises_performance_debugging
python -m scripts.09_exercises_incremental_delta_quality
python -m scripts.10_exercises_sql_views
```

---

## Delta Lake (local pip PySpark)

Exercises in `09_exercises_incremental_delta_quality.py` use **Delta MERGE**. Plain pyspark alone can’t do MERGE.

### Install pinned compatible versions
Pick one:

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

### IMPORTANT: enable Delta in SparkSession
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

A lot of DE teams expect you to have at least basic **unit tests** for transforms + data quality logic.

### Install test deps
```bash
pip install pytest
```

### Run tests
From repo root:
```bash
pytest -q
```

Tests live in:
- `tests/test_pipeline_logic.py`

---

## Azure-ize (how to talk about this like a real DE project)

This repo is intentionally local-first, but you should be able to explain how it becomes an Azure pipeline.

### Storage layout (ADLS Gen2)
Use a clean lake layout:
- `abfss://<container>@<account>.dfs.core.windows.net/bronze/customers/ingest_date=YYYY-MM-DD/`
- `abfss://<container>@<account>.dfs.core.windows.net/bronze/txns/ingest_date=YYYY-MM-DD/`
- `abfss://<container>@<account>.dfs.core.windows.net/silver/customers/`
- `abfss://<container>@<account>.dfs.core.windows.net/silver/txns/`  *(Delta)*
- `abfss://<container>@<account>.dfs.core.windows.net/gold/daily_kpis/` *(Delta)*

### Orchestration (ADF)
Typical ADF flow:
1. **Copy activity** pulls raw files into **Bronze** (partitioned by ingest_date)
2. **Databricks job** runs transforms:
   - Bronze → Silver (clean + quarantine + FK checks)
   - Silver MERGE into Delta tables (idempotent upserts)
   - Gold aggregates updated incrementally
3. **Logging** to a run log table (run_id, watermark_before/after, rows_in/out, quarantine counts)

### Security (Managed Identity)
- ADF + Databricks authenticate to ADLS using **Managed Identity**
- Grant ADLS permissions using **RBAC** (Storage Blob Data Contributor/Reader as appropriate)
- No secrets in code, no connection strings in scripts

### Backfills + retries (production mindset)
- Parameterize runs by `run_date` (or date range)
- Backfill by reprocessing a partition range
- Retries should be safe because Silver/Gold are updated with **MERGE** (idempotent)

### Cost basics (what interviewers like to hear)
- Use job clusters with autoscaling and termination
- Prefer Delta + partition pruning to reduce scan costs
- Avoid small files (coalesce/repartition before writes)
- Use broadcast joins for small dimensions; watch skew on big keys
