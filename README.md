# pyspark-local-intro — PySpark Training + Exercises (Local VS Code)

Intro PySpark training + small exercises you can run locally using VS Code.

This is built for:
- fast setup
- clean file structure
- short scripts you can re-run
- practical exercises that mirror real DE tasks (cleaning, dedupe, aggregations, joins, QA checks)

---

## 0) Prereqs

- **Python 3.10+** recommended
- **Java (required by Spark)**

### Install Java (pick one)
- **Windows:** install a JDK (Temurin/OpenJDK). Ensure `java -version` works.
- **macOS:** `brew install openjdk`
- **Ubuntu/Debian:** `sudo apt-get update && sudo apt-get install -y default-jdk`

Verify:
```bash
python --version
java -version
```

> If `java -version` fails, Spark will not start. Fix Java first.

---

## 1) Project Structure

```text
pyspark-local-intro/
├─ data/
│  ├─ raw/                  # small raw CSVs
│  │  ├─ customers.csv
│  │  └─ transactions.csv
│  └─ out/                  # outputs written by Spark (parquet/csv)
├─ notebooks/               # optional (if you want Jupyter later)
├─ scripts/
│  ├─ 00_smoke_test.py      # confirms Spark runs locally
│  ├─ 01_basics_df.py       # DataFrame basics, schema, select/filter
│  ├─ 02_cleaning.py        # nulls, casting, trimming, rules + quarantine
│  ├─ 03_aggregations.py    # groupBy + window + KPIs
│  ├─ 04_joins.py           # join dims + facts (DE pattern)
│  └─ 05_exercises.py       # TODO tasks (you fill these in)
├─ src/
│  └─ spark_utils.py        # shared SparkSession builder
├─ .gitignore
├─ requirements.txt
└─ README.md
```

---

## 2) VS Code Setup (Virtual Env)

### Windows (PowerShell)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### WSL/Linux/macOS
```bash
rm -rf .venv                            # Delete the existing .venv

python3 -m venv .venv                   # Create the venv inside WSL
source .venv/bin/activate               # Activate the venv (WSL)

which python                            #Check Python:
python --version

python3 -m pip install --upgrade pip    # Install deps inside the venv
```

### VS Code: select the right interpreter
- `Ctrl+Shift+P` → **Python: Select Interpreter**
- Choose the one under `.venv`

---

## 3) Install Dependencies

From repo root:
```bash
pip install -r requirements.txt
```

Recommended `requirements.txt`:
```txt
pyspark==3.5.1
pandas==2.2.2
pyarrow==17.0.0
```

---

## 4) Add Sample Data (tiny CSVs)

Create:

### `data/raw/customers.csv`
```csv
customer_id,first_name,last_name,state,signup_date
1,Frank,Runfola,NY,2025-01-10
2,Ana,Lopez,NC,2025-02-03
3,Sam,Kim,CA,2025-02-15
4,,Patel,TX,2025-03-01
5,Jen,Chen,CA,2025-03-05
```

### `data/raw/transactions.csv`
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

## 5) Create Shared Spark Session Helper

Create:

### `src/spark_utils.py`
```python
# src/spark_utils.py
# Shared SparkSession builder so every script is consistent.

from pyspark.sql import SparkSession

def get_spark(app_name: str = "pyspark-local-intro") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Local mode; use all CPU cores available
        .master("local[*]")
        # Keep local runs snappy and predictable
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
```

---

## 6) Run the Training (in order)

From repo root:
```bash
# RUN THESE SCRIPTS AS MODULES 
# Folders Require  __init__.py  (scripts/__init__.py and src/__init__.py )
python -m scripts.00_smoke_test
python -m scripts.01_basics_df
python -m scripts.02_cleaning
python -m scripts.03_aggregations
python -m scripts.04_joins
python -m scripts.05_exercises

```

Outputs will land in:
- `data/out/`

> Spark writes outputs as **folders** (not a single file). That’s normal.

---

## 7) What You’ll Learn (Quick Map)

### 00 — Smoke test
- SparkSession starts
- you can create and show a DataFrame

### 01 — DataFrame basics
- read CSV
- schema inspection
- select / filter / derived columns

### 02 — Cleaning + quarantine
- trimming + standardizing text
- quarantine bad records (real DE habit)

### 03 — Aggregations + window
- groupBy KPIs
- rank customers by spend

### 04 — Joins
- fact + dimension join pattern
- analytics table (one row per customer)

### 05 — Exercises (you do it)
- enforce rules
- create KPI table
- write outputs
- print a run summary

---

## 8) Exercises (What to Implement)

In `scripts/05_exercises.py`, fill in the TODOs:

1) **Fix customer names**
- trim first/last
- create `full_name`

2) **Quarantine transaction rules**
- quarantine if `amount <= 0`
- quarantine if `customer_id` does **not** exist in customers list
  - hint: `left_anti` join is your friend

3) **Customer KPI table**
- one row per `customer_id`
- columns:
  - `txn_count`
  - `total_spend`
  - `last_txn_ts`
- show top 10 by spend

4) **Write outputs**
- write `txns_clean2` to `data/out/txns_clean2`
- write `customer_kpis2` to `data/out/customer_kpis2`

Keep it readable. The goal is clean logic, not fancy tricks.

---

## 9) Suggested `.gitignore`

```gitignore
.venv/
__pycache__/
*.pyc
data/out/
.vscode/
.DS_Store
```

---

## 10) Common Problems (and fixes)

- **Java not found** → Spark won’t start  
  Fix: install a JDK and confirm `java -version`.

- **VS Code using wrong Python** → imports fail / packages missing  
  Fix: select `.venv` interpreter.

- **Running from the wrong folder** → Spark can’t find `data/raw/...`  
  Fix: always run commands from repo root.

- **Spark output is a folder** → “why isn’t it one file?”  
  That’s Spark. A “dataset” is written as a directory of part files.

---

## 11) Next Step (turn this into portfolio-grade)

If you want this to look like real DE work without making it huge:

- Add a **run summary** at the end of each script:
  - rows in
  - rows clean
  - rows quarantined
  - output path written
- Add a basic `Makefile` or `run.ps1`:
  - `make run` runs scripts in order
- Add a mini “Bronze / Silver / Gold” output pattern:
  - `data/out/bronze/...`
  - `data/out/silver/...`
  - `data/out/gold/...`
- Add an “Azure-ize” note:
  - same code, just replace local paths with ADLS paths
  - run on Databricks, write Delta/Parquet to ADLS

That’s the exact jump from “practice” to “hire me”.
