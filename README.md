# Olympic Data Platform

A local batch data platform built with **Python and PySpark** using the Olympic historical dataset.

The project ingests raw CSV files, applies data cleaning and standardization, builds a dimensional model, manages historical NOC changes using **SCD Type 2**, and produces analytics-ready Parquet tables.

## Architecture

The pipeline follows a medallion-style architecture:

```text
CSV source files
      ↓
Bronze — raw data with ingestion metadata
      ↓
Silver — cleaned, typed and standardized data
      ↓
Gold — star schema for analytics
```

### Bronze

The Bronze layer preserves the source data and adds:

* ingestion timestamp;
* source filename;
* batch date;
* corrupt-record capture.

Bronze tables are partitioned by `_batch_date`.

### Silver

The Silver layer:

* trims and standardizes text;
* converts missing values to null;
* casts columns to their correct data types;
* removes duplicate records;
* standardizes NOC codes;
* generates record hashes for change detection.

A governed mapping is used to resolve differences between source systems, such as:

```text
SIN → SGP
```

### Gold

The Gold layer contains a star schema:

```text
                    dim_athlete
                         |
dim_games ───── fact_participation ───── dim_event
                         |
                      dim_noc
```

Tables:

* `fact_participation`
* `dim_athlete`
* `dim_games`
* `dim_event`
* `dim_noc`

The fact-table grain is:

> One athlete participating in one Olympic event, during one Games edition, representing one NOC.

The fact table is partitioned by `year`.

## SCD Type 2

`dim_noc` uses Slowly Changing Dimension Type 2 to preserve region history.

The following fields support versioning:

* `valid_from`
* `valid_to`
* `is_current`
* `record_hash`

When a tracked region value changes:

1. the previous record is closed;
2. `is_current` becomes `false`;
3. `valid_to` is updated;
4. a new current version is inserted.

The `noc_code` remains the business key, while `noc_key` identifies each dimension version.

## Project structure

```text
Olympic-dataplatform-challenge/
├── data/
│   ├── input/
│   ├── reference/
│   └── output/
│       └── bronze/
│       └── silver/
│       └── gold/
│   └── test_input/
│   └── test_output/
├── notebooks/
├── src/
│   └── olympic_pipeline/
│       ├── __init__.py
│       ├── basic.py
│       ├── dimensions.py
│       ├── facts.py
│       ├── hashing.py
│       ├── ingestion.py
│       ├── main.py
│       ├── quality_checks.py
│       ├── reference_data.py
│       ├── scd_type_2.py
│       ├── schemas.py
│       ├── silver.py
│       └── spark_session.py
├── requirements.txt
└── README.md
```

## Requirements

* Python 3.12 or compatible version
* Java
* PySpark
* pytest
* mypy
* Ruff

Create and activate a virtual environment:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

Install the project dependencies:

```powershell
pip install -r requirements.txt
pip install -e .
```

## Running the pipeline

The pipeline receives:

1. input directory;
2. reference-data directory;
3. output directory (is created during the process);
4. batch date.

Example:

```powershell
python -m olympic_pipeline.main data/test_input/batch_1 data/reference data/test_output 2026-06-20
```

Run the second batch without deleting the first output:

```powershell
python -m olympic_pipeline.main data/test_input/batch_2 data/reference data/test_output 2026-06-21
```

To delete all the created directories and files stored after the runs use : 

```powershell
if (Test-Path "data\test_output") {
    Remove-Item -Recurse -Force "data\test_output"
}
```

Keeping the same output directory allows the pipeline to compare the new NOC data with the existing dimension and create SCD Type 2 versions.

## Data quality checks

The pipeline validates:

* tables are not empty;
* required business keys are not null;
* fact foreign keys match dimension primary keys;
* every NOC has exactly one current version;
* reference mappings do not create ambiguous relationships.

A successful foreign-key validation confirms that every foreign-key value in `fact_participation` has a matching primary key in the corresponding dimension.


## Code quality

```powershell
python -m mypy src
```

## Analytics

The Gold tables support analyses such as:

* medals won by country;
* athlete medal records by country;
* participation by year;
* SCD Type 2 inserts and historical changes;
* foreign-key integrity validation.

Notebook examples are stored in the `notebooks` directory.

## Dataset note

The source dataset contains one row per athlete-event participation. In team events, the same country medal may therefore appear once for each team member. Athlete medal records can be counted directly, while a country-level medal table should first deduplicate records by Olympic edition, NOC, team, sport, event and medal type.

Synthetic NOC region changes used in the demonstration batches exist only to demonstrate SCD Type 2 behaviour and do not represent official historical changes.
