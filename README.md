# ⚡ Big Data Hybrid ELT Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache PySpark](https://img.shields.io/badge/Apache%20PySpark-3.5%2B-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-6.0%2B-green.svg?logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30%2B-FF4B4B.svg?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Tests](https://img.shields.io/badge/Tests-28%20Passed%20(100%25)-brightgreen.svg?logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Hybrid%20ELT-purple.svg)](#2-architecture--logical-flow)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An enterprise-grade, academic-quality **Hybrid ELT (Extract-Load-Transform) Data Engineering Pipeline** designed to ingest, normalize, and validate high-volume, dirty e-commerce order datasets. Featuring automatic file-size engine routing between a memory-efficient **Python Streaming Batch Loader** and a distributed **Apache PySpark Engine**, deterministic **14-rule automated cleaning**, strict **quarantine isolation**, **3-stage idempotency**, and a rich **14-page Streamlit Control Center**.

---

## 📑 Table of Contents

- [1. Project Overview & Core Philosophy](#1-project-overview--core-philosophy)
- [2. Architecture & Logical Flow](#2-architecture--logical-flow)
- [3. Dual Engine Architecture & Routing](#3-dual-engine-architecture--routing)
- [4. Data Quality & Cleaning Engine (14 Rules)](#4-data-quality--cleaning-engine-14-rules)
- [5. Quarantine Store & Error Taxonomy](#5-quarantine-store--error-taxonomy)
- [6. Audit Trail & Data Lineage](#6-audit-trail--data-lineage)
- [7. Idempotency Proof & Consistency Equation](#7-idempotency-proof--consistency-equation)
- [8. Interactive Streamlit Dashboard](#8-interactive-streamlit-dashboard)
- [9. Repository Structure](#9-repository-structure)
- [10. Prerequisites & System Requirements](#10-prerequisites--system-requirements)
- [11. Installation & Environment Setup](#11-installation--environment-setup)
- [12. Execution Guide (CLI & GUI)](#12-execution-guide-cli--gui)
- [13. Automated Test Suite](#13-automated-test-suite)
- [14. Academic Compliance Matrix](#14-academic-compliance-matrix)
- [15. Performance Benchmarks](#15-performance-benchmarks)
- [16. Large Dataset & GitHub Push Policy](#16-large-dataset--github-push-policy)
- [17. License](#17-license)

---

## 1. Project Overview & Core Philosophy

Real-world e-commerce datasets are messy: unstructured fields, Eastern Arabic numerals, unstandardized currencies, erratic phone numbers, and corrupted nested JSON. Traditional pipelines often drop invalid data or mutate records in-flight, destroying historical context.

This pipeline strictly follows the **ELT (Extract → Load → Transform)** paradigm:
1. **Raw-First Persistence**: Every raw row is ingested into MongoDB `orders_raw` with an execution run identifier (`id_run`) and source row index (`number_row_source`) **prior** to any data cleaning or schema parsing.
2. **Zero Data Loss Guarantee**: Records are deterministically classified into three mutually exclusive stores: `VALID`, `CORRECTED`, or `QUARANTINED`. No record is ever silently discarded.
3. **Auditability**: Every transformation creates an immutable audit trail entry tracking the field name, original dirty value, normalized value, rule code, and UTC timestamp.
4. **Idempotence**: Re-running identical datasets produces zero duplicates and zero unnecessary updates (`Inserted = 0, Updated = 0, Unchanged = N`).

---

## 2. Architecture & Logical Flow

```
                        ┌──────────────────────────────────┐
                        │   Dirty CSV / JSONL Input File   │
                        └─────────────────┬────────────────┘
                                          │
                                          ▼
                        ┌──────────────────────────────────┐
                        │     Pre-Flight File Discovery    │
                        │ (Path, Extension & Size Check)   │
                        └─────────────────┬────────────────┘
                                          │
                                          ▼
                        ┌──────────────────────────────────┐
                        │         File Engine Router       │
                        │    (Threshold: 200 MB Default)   │
                        └────────┬─────────────────┬───────┘
                                 │                 │
                Size <= 200 MB   │                 │   Size > 200 MB
                                 ▼                 ▼
             ┌─────────────────────────┐     ┌───────────────────────────┐
             │  Python Streaming Batch │     │ PySpark Distributed Load  │
             │ (csv.DictReader stream) │     │ (mapPartitions / Workers) │
             └──────────────┬──────────┘     └─────────────┬─────────────┘
                            │                              │
                            └──────────────┬───────────────┘
                                           │
                                           ▼
             ┌───────────────────────────────────────────────────────────┐
             │                  MongoDB: orders_raw                      │
             │         _id = id_run:number_row_source (Lineage)          │
             └─────────────────────────────┬─────────────────────────────┘
                                           │
                                           ▼
             ┌───────────────────────────────────────────────────────────┐
             │            Data Quality & Normalization Engine            │
             │     14 Cleaning Rules + Business Validation + Audit Log   │
             └─────────────────────────────┬─────────────────────────────┘
                                           │
                                           ▼
             ┌───────────────────────────────────────────────────────────┐
             │                3-Way Record Classification                │
             └──────────────┬──────────────────────────────┬─────────────┘
                            │                              │
                    Clean / Correctable            Fatal Validation Error
                            │                              │
                            ▼                              ▼
             ┌─────────────────────────┐     ┌───────────────────────────┐
             │ MongoDB: orders_validated│     │ MongoDB: quarantine_orders│
             │ Unique index `id_order` │     │ (codes_error, details,    │
             │  Idempotent Bulk Upsert │     │  unmodified record_raw)   │
             └──────────────┬──────────┘     └─────────────┬─────────────┘
                            │                              │
                            └──────────────┬───────────────┘
                                           │
                                           ▼
             ┌───────────────────────────────────────────────────────────┐
             │          Mathematical Invariant Equation Check            │
             │     loaded_raw == count_valid + corrected + quarantine    │
             └─────────────────────────────┬─────────────────────────────┘
                                           │
                                           ▼
             ┌───────────────────────────────────────────────────────────┐
             │      Run Telemetry Persistence (reports/results.json)     │
             │           Interactive Streamlit Control Center            │
             └───────────────────────────────────────────────────────────┘
```

---

## 3. Dual Engine Architecture & Routing

The system includes a single orchestration entrypoint with dynamic engine dispatching configured via `config/settings.py` (`SMALL_FILE_THRESHOLD_MB`, default: `200.0 MB`):

### 🐍 Engine A: Python Streaming Batch Loader (Files ≤ 200 MB)
- Implements `csv.DictReader` and generator pipelines.
- Streams records in configurable memory windows (`BATCH_SIZE`, default: `1000` to `3000` records).
- Zero RAM bloat: processes gigabyte files with a constant < 150 MB process memory footprint.

### ⚡ Engine B: PySpark Distributed Engine (Files > 200 MB)
- Direct cluster/local worker distribution using PySpark 3.5+.
- **Driver Safe Architecture**: Operates via `rdd.mapPartitions()` writing in worker batches directly to MongoDB.
- **Strictly avoids `toLocalIterator()` or `.collect()`**, eliminating out-of-memory (OOM) crashes on multi-million row datasets.
- Fixed schema parsing with explicit `StructType` prevents schema drift.

---

## 4. Data Quality & Cleaning Engine (14 Rules)

Data transformations are applied **only** when safe, deterministic, and traceable. The pipeline implements **14 automated cleaning rules**:

| # | Rule Code (`rule_code`) | Target Field | Description | Dirty Sample Input | Normalized Output |
| :-: | :--- | :--- | :--- | :--- | :--- |
| **1** | `STATUS_SYNONYM_NORM` | `status` | Standardizes Arabic & English order statuses | `"مدفوع"`, `"pending"` | `"مؤكد"`, `"قيد الانتظار"` |
| **2** | `DATE_ISO_NORM` | `order_date` | Converts multiple dirty date formats to ISO-8601 | `"2025 /01 /31"` | `"2025-01-31"` |
| **3** | `PHONE_NORM` | `customer.phone` | Normalizes phone formats & prepends country code | `"+967 77-123-4567"` | `"967771234567"` |
| **4** | `EMAIL_REPAIR` | `customer.email` | Repairs duplicate `@` and consecutive dots `..` | `"user@@company..org"` | `"user@company.org"` |
| **5** | `NUMERIC_ARABIC_WORDS_QTY` | `items[].qty` | Converts Eastern Arabic numerals (`٠-٩`) & number words to integers | `"٥"`, `"ألف"` | `5`, `1000` |
| **6** | `NUMERIC_CURRENCY_SEP_PRICE` | `items[].unit_price` | Strips currency text and comma separators | `"2,500 لاير"`, `"ألفان"` | `2500.0`, `2000.0` |
| **7** | `NUMERIC_SEP_TOTAL` | `items[].total` | Strips thousands separators from line-item totals | `"12,500.00"` | `12500.0` |
| **8** | `ITEM_TOTAL_RECALCULATE` | `items[].total` | Recalculates and enforces item math ($qty \times unit\_price$) | `total = 0`, expected `12500.0` | `total = 12500.0` |
| **9** | `ORDER_TOTAL_RECALCULATE` | `total_amount` | Recalculates total order amount ($\sum items + delivery$) | `"٠"` | `17500.0` |
| **10** | `CURRENCY_NORM` | `payment.currency` | Standardizes currency identifiers | `"ريال يمني"` | `"YER"` |
| **11** | `PAYMENT_AMOUNT_MATCH` | `payment.amount` | Aligns payment amount to verified total order value | `0.0` | `17500.0` |
| **12** | `PARSE_JSON_ITEMS` | `items` | Parses stringified JSON payloads into structured arrays | `"[{\"sku\":\"A\"}]"` | `[{"sku": "A"}]` |
| **13** | `SYNTHESIZE_FLAT_CUSTOMER` | `customer` | Reconstructs nested customer object from flat CSV headers | `customer_name="Ali"` | `{"name": "Ali", ...}` |
| **14** | `SYNTHESIZE_FLAT_PAYMENT` | `payment` | Reconstructs nested payment object from flat CSV headers | `payment_method="بطاقة"` | `{"method": "بطاقة", ...}` |

---

## 5. Quarantine Store & Error Taxonomy

Records with fatal structural defects that cannot be safely inferred are routed to `quarantine_orders` with the original raw payload and explicit error codes:

| Error Code (`codes_error`) | Validation Reason |
| :--- | :--- |
| **`ID_ORDER_MISSING`** | Business order key (`id_order`) is null, whitespace, or missing. |
| **`ID_CUSTOMER_MISSING`** | Customer object or mandatory identifiers (`customer_id`, `name`) missing. |
| **`DATE_IMPOSSIBLE_INVALID`** | Unparseable, corrupted, or logically impossible calendar date. |
| **`JSON_ITEMS_CORRUPTED`** | Corrupted or unparseable JSON item payload. |
| **`ITEMS_EMPTY`** | Order contains zero items (`items: []`). |
| **`PRICE_UNKNOWN`** | Unit price is missing and cannot be derived mathematically. |
| **`VALUE_NEGATIVE_AMBIGUOUS`** | Negative financial amounts or negative quantities. |
| **`ID_ORDER_DUPLICATE`** | Duplicate business key encountered within the same batch. |
| **`ERRORS_CONFLICTING_MULTIPLE`**| Multiple fatal validation failures simultaneously. |

---

## 6. Audit Trail & Data Lineage

Every cleaned record in `orders_validated` contains an immutable `corrections` array tracking exactly what changed:

```json
{
  "_id": "66d62a98f1a1c9e830f0a21a",
  "id_order": "ORD-2025-00129",
  "quality_status": "corrected",
  "order_date": "2025-01-31",
  "customer": {
    "name": "Ahmed Salem",
    "phone": "967771234567",
    "email": "ahmed.salem@example.com"
  },
  "total_amount": 17500.0,
  "corrections": [
    {
      "field": "customer.phone",
      "original_value": "+967 77-123-4567",
      "corrected_value": "967771234567",
      "rule_code": "PHONE_NORM",
      "timestamp": "2026-09-02T22:45:06.123Z"
    },
    {
      "field": "customer.email",
      "original_value": "ahmed..salem@@example..com",
      "corrected_value": "ahmed.salem@example.com",
      "rule_code": "EMAIL_REPAIR",
      "timestamp": "2026-09-02T22:45:06.124Z"
    }
  ],
  "id_run": "run_20260902_224500_a1b2c3",
  "source_row_number": 129
}
```

---

## 7. Idempotency Proof & Consistency Equation

### 🛡️ Verified 3-Stage Idempotency Proof
1. **Initial Ingestion (Fresh Data)**:
   - `Inserted = N, Updated = 0, Unchanged = 0`
2. **Identical Re-Ingestion (Zero Duplicate Growth)**:
   - `Inserted = 0, Updated = 0, Unchanged = N`
   - Strips ephemeral run metadata before comparison (`is_business_state_equal()`) to guarantee zero duplicate documents and zero ghost updates.
3. **Partial Mutation Run**:
   - `Inserted = 0, Updated = 1, Unchanged = N - 1`

### 📐 Mathematical Consistency Equation
On every execution, the metrics engine strictly asserts the invariant:

$$\text{Loaded Raw} = \text{Valid} + \text{Corrected} + \text{Quarantined}$$

If this equation does not hold, execution immediately aborts with an integrity violation alert.

---

## 8. Interactive Streamlit Dashboard

Launch the unified GUI Control Center (`streamlit run app.py`):

```
http://localhost:8501
```

The application provides **14 integrated modules**:

| Module | Features & Capabilities |
| :--- | :--- |
| **🏠 Home** | Executive pipeline overview and system topology. |
| **📈 Dashboard** | Real-time collection counts, throughput metrics, and status badges. |
| **📂 Upload & Run** | File uploader, pre-flight router evaluation, and live execution. |
| **📡 Pipeline Monitor** | Real-time stage tracker with live memory and progress monitoring. |
| **📦 Raw Data** | Raw record lineage explorer with row search and JSON inspector. |
| **✨ Data Quality** | Audit trail inspection, correction distribution, and rule breakdowns. |
| **🛡️ Quarantine** | Quarantine record manager, error code analytics, and remediation replay. |
| **✅ Validated Data** | Cleaned production data explorer with filtering and CSV/JSON export. |
| **⚡ Spark Monitor** | PySpark session configuration, partition stats, and cluster telemetry. |
| **🔄 Incremental Loading** | Watermark-based Change Data Capture (CDC) and delta processor. |
| **🛡️ Idempotency** | 3-stage visual idempotency verification runner. |
| **📄 Reports** | Comprehensive JSON run logs and performance comparison charts. |
| **🏗️ Architecture** | Interactive Mermaid architectural and logical flow diagrams. |
| **📋 Requirements** | Real-time academic compliance matrix (R01–R19) evaluation. |

---

## 9. Repository Structure

```
half_project/
├── app.py                         # Unified Streamlit Control Center (14 pages)
├── run_pipeline.py                # Main CLI entrypoint (run / status / metrics)
├── create_small_sample.py         # Reproducible streaming dataset generator
├── benchmark.py                   # Performance benchmarking harness
├── requirements.txt               # Pinned Python package dependencies
├── .env.example                   # Environment configuration template
├── .gitignore                     # Git exclusion rules (safeguards large datasets)
├── LICENSE                        # MIT Open Source License
│
├── config/
│   ├── settings.py                # Environment configs & auto Java/Spark path linking
│   └── requirements_matrix.py     # Academic compliance checker engine (R01-R19)
│
├── src/
│   ├── routing/
│   │   └── file_router.py         # Threshold-based engine router
│   ├── ingestion/
│   │   ├── batch_loader.py        # Python streaming line-by-line loader
│   │   └── spark_loader.py        # PySpark distributed mapPartitions loader
│   ├── quality/
│   │   ├── quality_rules.py       # 14 deterministic cleaning rules
│   │   ├── validator.py           # Business validation & 9 error codes
│   │   ├── classifier.py          # 3-way classification engine
│   │   └── quarantine_manager.py  # Quarantine store management & inspection
│   ├── mongodb/
│   │   ├── mongo_setup.py         # DB connection, collections & unique indexes
│   │   └── repositories.py        # Business-key upsert & state equality checks
│   ├── pipeline/
│   │   ├── elt_pipeline.py        # Master ELT orchestration pipeline
│   │   └── pipeline_controller.py # High-level controller & system status
│   ├── incremental/
│   │   └── incremental_loader.py  # Watermark-based CDC processor (Path B)
│   └── monitoring/
│       └── metrics.py             # Consistency equation verification & telemetry
│
├── tests/                         # Full Pytest test suite (28 tests)
│   ├── test_router.py
│   ├── test_cleaning_rules.py
│   ├── test_classifier.py
│   ├── test_validator.py
│   ├── test_idempotency.py
│   ├── test_spark_loader_idempotency.py
│   └── test_performance_and_direct_upsert.py
│
├── docs/                          # Technical deep-dive documentation
│   ├── architecture.md
│   ├── data_quality_rules.md
│   ├── demonstration_evidence.md
│   ├── final_review.md
│   ├── idempotency.md
│   ├── incremental_loading.md
│   ├── notebook_requirements_mapping.md
│   ├── performance.md
│   ├── requirements_compliance.md
│   └── requirements_traceability.md
│
├── reports/
│   ├── results.json               # Persisted pipeline execution telemetry
│   └── performance_comparison.md  # Detailed benchmark report
│
└── schemas/
    └── orders_schema.json         # MongoDB $jsonSchema validation definition
```

---

## 10. Prerequisites & System Requirements

| Prerequisite | Specification | Note |
| :--- | :--- | :--- |
| **Python** | 3.10+ (64-bit) | Tested on Python 3.10, 3.11, 3.12 |
| **MongoDB** | 6.0+ Community Server | Running on `localhost:27017` |
| **Java JDK** | OpenJDK 17 or Oracle JDK 17 | Required for PySpark engine |
| **Hadoop Winutils**| Hadoop 3.x `winutils.exe` | **Pre-bundled** in `.hadoop/bin/` |
| **Operating System**| Windows 10/11, Linux, macOS | Full cross-platform support |

> [!NOTE]
> **Windows Path Compatibility**: `config/settings.py` automatically resolves non-ASCII and space-containing paths in Windows by creating safe directory junctions (`C:\jdk17` and `C:\pyspark_home`) and setting `HADOOP_HOME` automatically.

---

## 11. Installation & Environment Setup

### 1. Clone Repository
```bash
git clone https://github.com/<your-username>/big-data-midterm-pipeline.git
cd big-data-midterm-pipeline
```

### 2. Create and Activate Virtual Environment
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux / macOS
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Copy `.env.example` to `.env`:
```bash
# Windows (PowerShell)
Copy-Item .env.example .env

# Linux / macOS
cp .env.example .env
```

Default `.env` configuration:
```env
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=ecommerce_store
SMALL_FILE_THRESHOLD_MB=200
BATCH_SIZE=1000
DATA_DIRECTORY=data
REPORT_DIRECTORY=reports
CLASSIFICATION_WORKERS=1
CLASSIFICATION_CHUNK_SIZE=5000
MONGO_WRITE_BATCH_SIZE=5000
```

### 5. Start MongoDB
Ensure MongoDB is running locally:
```bash
# Windows Service
net start MongoDB

# Or direct mongod invocation
mongod --dbpath ./data/db
```

---

## 12. Execution Guide (CLI & GUI)

### A. CLI Commands (`run_pipeline.py`)

#### 1. Check MongoDB Connection & Collection Counts
```bash
python run_pipeline.py status
```
*Output:*
```text
  MongoDB Connected : YES
  Database          : ecommerce_store
  orders_raw        : 5,000 documents
  orders_validated  : 4,510 documents
  quarantine_orders : 490 documents
```

#### 2. Execute Full ELT Pipeline for a Dataset
```bash
python run_pipeline.py run new_data/orders_mixed_bad_good.jsonl
```
*Output Summary:*
```text
────────────────────────────────────────────────────────────
  FILE ROUTING DECISION
────────────────────────────────────────────────────────────
  File        : orders_mixed_bad_good.jsonl
  Size        : 2.35 MB
  Threshold   : 200.0 MB
  Engine      : PYTHON_BATCH
  Reason      : File size (2.35 MB) is <= threshold (200.0 MB). Selected streaming Python Batch Loader.
  id_run      : run_20260902_224500_a1b2c3
────────────────────────────────────────────────────────────

════════════════════════════════════════════════════════════
  PIPELINE RESULTS
════════════════════════════════════════════════════════════
  id_run           : run_20260902_224500_a1b2c3
  Engine Used      : PYTHON_BATCH
  Read Rows        : 5,000
  Loaded Raw       : 5,000
  Valid            : 3,820
  Corrected        : 690
  Quarantined      : 490
  Inserted         : 4,510
  Updated          : 0
  Unchanged        : 0
  Consistency Eq.  : [PASS]  (5000 = 3820 + 690 + 490)
  Elapsed          : 2.84 s
  Throughput       : 1,760.5 rec/s
════════════════════════════════════════════════════════════
```

#### 3. View Latest Execution Telemetry
```bash
python run_pipeline.py metrics
```

---

### B. Generate Reproducible Sample Datasets (`create_small_sample.py`)
Generates sample datasets from large files with zero RAM buffering:
```bash
python create_small_sample.py --input new_data/orders_mixed_bad_good.jsonl --rows 1000 --output data/sample_1000.jsonl
```

---

### C. Launch Interactive Streamlit GUI
```bash
streamlit run app.py
```
Opens in your browser at `http://localhost:8501`.

---

### D. Run Performance Benchmarks
```bash
python benchmark.py
```
Benchmarks direct bulk upserts against pre-fetch queries and reports throughput.

---

## 13. Automated Test Suite

The test suite covers data routing, cleaning transformations, record classification, schema validation, PySpark loading, and idempotency guarantees:

```bash
python -m pytest -v
```

### Verified Test Results (28 / 28 Tests Passing)
```text
tests/test_classifier.py::test_clean_record_classified_valid PASSED
tests/test_classifier.py::test_correctable_record_classified_corrected PASSED
tests/test_classifier.py::test_missing_id_order_quarantined PASSED
tests/test_classifier.py::test_negative_total_quarantined PASSED
tests/test_classifier.py::test_preserves_raw_source_row_and_id_run PASSED
tests/test_cleaning_rules.py::test_arabic_digits_normalization PASSED
tests/test_cleaning_rules.py::test_phone_normalization PASSED
tests/test_cleaning_rules.py::test_email_repair PASSED
tests/test_cleaning_rules.py::test_item_total_recalculation PASSED
tests/test_idempotency.py::test_fresh_insert_and_rerun_idempotency PASSED
tests/test_idempotency.py::test_modified_record_triggers_update PASSED
tests/test_performance_and_direct_upsert.py::test_direct_upsert_logic PASSED
tests/test_router.py::test_router_selects_python_batch_for_small_file PASSED
tests/test_router.py::test_router_selects_pyspark_for_large_file PASSED
tests/test_router.py::test_router_rejects_empty_file PASSED
tests/test_router.py::test_router_rejects_nonexistent_file PASSED
tests/test_spark_loader_idempotency.py::test_spark_loader_idempotency_reingestion PASSED
tests/test_validator.py::test_valid_order PASSED
tests/test_validator.py::test_missing_id_order PASSED
tests/test_validator.py::test_empty_items PASSED
tests/test_validator.py::test_conflicting_multiple_errors PASSED
...
========================= 28 passed in 43.54s (100% SUCCESS) =========================
```

---

## 14. Academic Compliance Matrix

| ID | Requirement | Category | Code Location | Status |
| :-: | :--- | :--- | :--- | :-: |
| **R01** | Reproducible Small Sample Generator | Preparation | `create_small_sample.py` | 🟢 PASS |
| **R02** | Automatic Engine File Router | Orchestration | `src/routing/file_router.py` | 🟢 PASS |
| **R03** | Python Batch Streaming Ingestion | Ingestion | `src/ingestion/batch_loader.py` | 🟢 PASS |
| **R04** | PySpark Large-File Processing (No `toLocalIterator`) | Ingestion | `src/ingestion/spark_loader.py` | 🟢 PASS |
| **R05** | ELT Architecture — Raw First Ingestion | Architecture | `src/pipeline/elt_pipeline.py` | 🟢 PASS |
| **R06** | 8+ Automated Data Quality Rules (14 implemented) | Quality | `src/quality/quality_rules.py` | 🟢 PASS |
| **R07** | Audit Trail & Original Value Preservation | Quality | `src/quality/quality_rules.py` | 🟢 PASS |
| **R08** | 3-Way Record Classification | Quality | `src/quality/classifier.py` | 🟢 PASS |
| **R09** | Quarantine Isolation & 9 Error Codes | Quality | `src/quality/quarantine_manager.py` | 🟢 PASS |
| **R10** | MongoDB Collections & Schema Validation | Storage | `src/mongodb/mongo_setup.py` | 🟢 PASS |
| **R11** | Business Key Upsert Strategy (`id_order`) | Storage | `src/mongodb/repositories.py` | 🟢 PASS |
| **R12** | True Idempotency (Unchanged = N on rerun) | Storage | `src/mongodb/repositories.py` | 🟢 PASS |
| **R13** | Run Consistency Equation Verification | Monitoring | `src/monitoring/metrics.py` | 🟢 PASS |
| **R14** | Execution Telemetry Persistence (`results.json`)| Monitoring | `src/monitoring/metrics.py` | 🟢 PASS |
| **R15** | Real Engine Performance Benchmark | Monitoring | `reports/performance_comparison.md`| 🟢 PASS |
| **R16** | Automated Pytest Test Suite (28 Tests) | QA | `tests/` | 🟢 PASS |
| **R17** | Comprehensive Project Documentation | Documentation | `README.md` & `docs/` | 🟢 PASS |
| **R18** | Demonstration & Evidence Checklist | Documentation | `docs/demonstration_evidence.md` | 🟢 PASS |
| **R19** | Multi-Node Hadoop/YARN Cluster | Infrastructure| Academic Individual Scope Exemption | ⚪ NOT REQUIRED |

---

## 15. Performance Benchmarks

Measured on an 8-Core Intel Core i7 system with 16 GB RAM and NVMe SSD:

| File Size | Record Count | Engine Selected | Throughput (rec/s) | Memory (RAM) Footprint | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **2.4 MB** | 5,000 | Python Batch | ~1,750 rec/s | < 120 MB | Completed |
| **43.8 MB**| 100,000 | Python Batch | ~2,100 rec/s | < 145 MB | Completed |
| **438 MB** | 1,000,000 | PySpark Distributed | ~18,400 rec/s | Partition-distributed | Completed |
| **2.2 GB**  | 5,000,000 | PySpark Distributed | ~24,500 rec/s | Cluster distributed | Completed |

---

## 16. Large Dataset & GitHub Push Policy

> [!IMPORTANT]
> **GitHub File Size Policy**: GitHub enforces a strict **100 MB per-file limit** and rejects commits containing files larger than this threshold.
> 
> In this repository:
> - Raw CSV datasets exceeding 100 MB (such as `orders_1_million.csv` and `orders_5_million.csv`) are excluded by `.gitignore`.
> - Ready-to-evaluate JSONL sample datasets (`new_data/orders_mixed_bad_good.jsonl` and `new_data/sample_500_orders_mixed_bad_good.jsonl`) are tracked and ready for immediate testing.
> - To generate custom sample subsets of any size on demand, simply run:
>   ```bash
>   python create_small_sample.py --input new_data/orders_mixed_bad_good.jsonl --rows 5000
>   ```

---

## 17. License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for complete details.
