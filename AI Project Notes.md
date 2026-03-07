# AI Project Notes — Electio-Analytics POC (MSPR Bloc 3) — Focus ETL + Data Lake

## 1) Project mission (POC)
Build a reproducible **data pipeline + zone-based (Medallion) data lake** for **Electio-Analytics** to support a later dashboard + ML.

### Business goal
Enable political consultants/strategists (non-technical users) to consume **historical trends and decision-ready indicators** for a single territory in the **Métropole de Lyon**, for **French presidential elections**.

### POC constraints
- **Fixed geography**: Métropole de Lyon only


## 2) Evaluation alignment (what must be visible)
The MSPR expects:
- Automated ETL with traceability and a named schema
- Structured storage (SQL compatibility acceptable)
- Data quality approach (checks + reports)
- Documentation: architecture diagram + conceptual model (MCD)
Security/RGPD must be addressed (political sensitivity → DPIA required in spec).

We focus now on: **ETL + Datalake + Warehouse-ready gold** (ML later).

## 3) Repository structure (current)
We keep the baseline structure:

electio-analytics-poc/
- config/
- data/{bronze,silver,gold}
- src/{ingestion,transformation,warehouse,ml,dashboard,utils}
- notebooks/
- docs/
- presentation/

### REQUIRED ADDITIONS (to match “raw/bronze/silver/gold” explicitly)
Add:
- data/raw/                 # untouched downloads (zip/csv/json)
- data/quality_reports/     # machine-readable check outputs
- src/quality/              # validation checks
- src/orchestration/        # run pipeline end-to-end

## 4) Medallion zones (data lake rules)
### RAW (data/raw)
- Exact downloads, unchanged.
- No cleaning. Never modify by hand.
- Naming: <domain>__<source>__<date>.<ext>

### BRONZE (data/bronze)
Purpose: “as ingested” structured copies with minimal parsing.

Not allowed:
- business logic, imputations, dedup beyond strict technical parsing

**Bronze metadata columns (mandatory):**
- ingestion_ts_utc (ISO string)
- source_name (string)
- source_url (string)
- geo_scope = "metropole_lyon"
- geo_granularity = one of {circonscription, commune, arrondissement}

### SILVER (data/silver)
Purpose: cleaned + standardized + conformed datasets ready to join.
Must:
- rename columns to snake_case
- cast types (numeric/date)
- handle missing values (strategy documented per dataset)
- remove duplicates (rule documented)
- harmonize geo keys (canonical geo_code)
- align time (year)
- filter to Métropole de Lyon (strict filter)

### GOLD (data/gold)
Purpose: business-ready warehouse layer + analytics datasets.
Must:
- provide reference data (dim tables)
- provide a fact table suitable for dashboard + later ML
- guarantee uniqueness constraints and quality checks

Gold is the ONLY source for analytics consumption (Dash) and later ML.


## 5) Target datasets (open data, France)
Ingestion will use public French datasets (e.g. data.gouv.fr, INSEE).
Domains:
- elections: presidential results historical
- security: crime / delinquency indicators
- employment: unemployment indicators
- demography: population, density
- poverty / socio-economic: poverty rate, income proxies
- economic life: number of companies (if available)

We only ingest aggregated/anonymized data.
No direct personal data. Political sensitivity → DPIA required by spec.

## 6) Warehouse-ready GOLD model (star schema)
Even if stored in Parquet, GOLD must be shaped like a warehouse:


## 7) ETL scripts responsibilities (must stay separated)
### src/ingestion/ingest_data.py  (RAW → BRONZE)
- Download/copy datasets into data/raw
- Parse to parquet into data/bronze
- Add bronze metadata columns
- Write a manifest: data/raw/manifest.json (source URLs, checksums if possible)

### src/transformation/clean_data.py (BRONZE → SILVER: cleaning)
- Standardize columns & types
- Missing values strategy (document)
- Remove duplicates
- Output clean silver per domain

### src/transformation/normalize.py (SILVER: conform)
- Create geo_code/geo_name consistently
- Filter to Métropole de Lyon
- Align time to year
- Output conformed silver tables (join-ready)


### src/warehouse/build_gold_layer.py (SILVER → GOLD)
- Build dim_geography + dim_time
- Build fact_elections_indicators
- Export gold parquet
- Optional: export SQL DB (SQLite) for “SQL compatible” delivery

### src/warehouse/create_star_schema.sql
- Provide DDL for a production SQL warehouse model (dims + fact).

## 8) Data quality (must output reports)
Implement in src/quality/ (e.g. checks.py) and write a report on every pipeline run to:
- data/quality_reports/<timestamp>_quality_report.json

Pipeline should STOP (raise error) if critical checks fail.

## 10) Orchestration (single command to rebuild everything)
Create src/orchestration/run_pipeline.py that runs:
1) ingest_data
2) clean_data
3) normalize
4) feature_engineering
5) build_gold_layer
6) run quality checks + write report

Goal: one command rebuilds the full lake from raw.

## 11) Security, legal, ethics (must be documented)
Per spec:
- Data localization: France or EU-compliant cloud
- Legal basis: legitimate interest
- DPIA required due to political sensitivity
- Retention: 1–3 years (POC)
- Risks: bias amplification, misuse, overconfidence
Mitigations (must be in docs/report later):
- Transparency: sources + methodology
- Disclaimers: indicative decision-support only
- Performance metrics (later, for ML)

For ETL now:
- store source URLs + licensing notes where possible in manifest
- ensure only aggregated/anonymized datasets are used
- avoid social media ingestion in POC unless explicitly justified
