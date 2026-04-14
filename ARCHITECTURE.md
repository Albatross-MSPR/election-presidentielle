# Project Architecture & Data Pipeline

## System Overview

This document describes the technical architecture and data flow of the French Presidential Election Analysis Project.

## Architecture Pattern: Medallion (Bronze-Silver-Gold)

The project follows a **medallion data architecture** pattern, which is industry-standard for data warehouses and data lakes.

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXTERNAL DATA SOURCES                        │
├──────────────────┬──────────────────┬──────────────────────────┤
│ data.gouv.fr     │ INSEE Data       │ Prefecture Police       │
│ Election API     │ Population/      │ Crime Indicators        │
│ 2017, 2022 T1/T2 │ Housing Data     │ 2021, 2022             │
└──────────────────┴──────────────────┴──────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│               RAW LAYER (Landing Zone)                          │
│  Purpose: Store unmodified source data as-is                    │
│  Format: Original (CSV, TXT)                                    │
│  Volume: Full source data                                       │
└─────────────────────────────────────────────────────────────────┘
                            ↓ (Download via requests.get)
                    [Ingestion Notebooks]
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│           BRONZE LAYER (Ingestion Layer)                        │
│  Purpose: Minimize transformations, add audit columns           │
│  Format: CSV (semicolon-delimited)                              │
│  Changes: Add source_url, ingestion_timestamp, source_file      │
│  Key Operations:                                                │
│    • Download from external APIs                                │
│    • Store raw data with minimal changes                        │
│    • Add metadata columns for governance                        │
│    • Filter to Rhône region (dept 69) where applicable          │
└─────────────────────────────────────────────────────────────────┘
                            ↓
                    [Transformation Notebooks]
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│           SILVER LAYER (Processing Layer)                       │
│  Purpose: Clean, validate, and standardize data                 │
│  Format: CSV (semicolon-delimited)                              │
│  Key Operations:                                                │
│    • Column name cleaning and standardization                   │
│    • Type conversions (to int, float, datetime)                 │
│    • String trimming and formatting                             │
│    • Remove duplicates                                          │
│    • Create calculated fields (percentages, derived metrics)    │
│    • Aggregate from voting bureau level to commune level        │
│    • Handle multiple candidates per row (11-16 candidates)     │
│    • Data validation and quality checks                         │
│  Output: Analysis-ready data                                    │
└─────────────────────────────────────────────────────────────────┘
                            ↓
                    [Orchestration Notebooks]
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│           GOLD LAYER (Analytics Layer)                          │
│  Purpose: Denormalized, optimized for analytics and ML          │
│  Format: Star schema with dimensions and facts                  │
│  Key Operations:                                                │
│    • Extract unique dimensions (candidates, communes, elections)│
│    • Create surrogate keys (IDs) for referential integrity      │
│    • Build fact tables joining all dimensions                   │
│    • Optimize query performance                                 │
│    • Add integration metadata (date_integration_gold)           │
│                                                                 │
│  Output Structure:                                              │
│    Dimension Tables:                                            │
│    • dim_election: {id_election, annee, tour}                  │
│    • dim_commune: {id_commune, code_dept, libelle_commune}    │
│    • dim_candidat: {id_candidat, nom, prenom, nuance}         │
│    • dim_indicateur_securite: {id, indicateur}                │
│                                                                 │
│    Fact Tables:                                                │
│    • fact_resultats: {id_commune, id_election, id_candidat,   │
│                       inscrits, abstentions, voix, ...}        │
│    • fact_securite: {id_commune, id_indicateur, annee,        │
│                      taux_pour_mille, insee_pop, ...}         │
└─────────────────────────────────────────────────────────────────┘
                            ↓
                    [ML Training Script]
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│           ML MODEL & PREDICTIONS                                │
│  Purpose: Predict electoral outcomes                            │
│  Algorithm: Logistic Regression (gradient descent)              │
│  Input: Gold layer features + target variable                   │
│  Output: Predictions and model metrics                          │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow by Processing Stage

### 1. Ingestion Stage
**Location:** `src/ingestion/`

**Inputs:** External APIs and files

**Process:**
```
External Source
    ↓
1. Download data via requests.get() or read file
2. Parse CSV/TXT with proper encoding and delimiters
3. Handle multi-row headers (clean up)
4. Filter to Rhône region (Code du département == "69")
5. Add audit columns:
   - extraction_source_url: Where data came from
   - ingestion_timestamp: When it was processed
   - source_file_name: Original filename
6. Save to BRONZE layer
```

**Notebooks:**
- `2017_pres_t1/2017-pres-t1-commune.ipynb` - 2017 T1 election results
- `2017_pres_t2/2017-pres-t2-commune.ipynb` - 2017 T2 election results
- `2022_pres_t1/2022-pres-t1-commune.ipynb` - 2022 T1 election results
- `2022_pres_t2/2022-pres-t2-commune.ipynb` - 2022 T2 election results
- `nuance politique/` - Political orientation mappings
- `ListeDesCommunes/` - Reference list of all French communes
- `DEP-departementale/` - Departmental-level data

**Key Challenges:**
- Multiple candidates per voting bureau row (up to 16)
- Dynamic column names with candidate numbers
- Different formats across years (header placement varies)
- Encoding issues (cp1252 for older files)

### 2. Transformation Stage
**Location:** `src/transformation/`

**Inputs:** BRONZE layer data

**Process:**
```
BRONZE data
    ↓
1. Load BRONZE CSV files
2. Clean column names:
   - Strip whitespace
   - Standardize naming conventions
3. Type conversions:
   - Count columns to integer (inscrits, voix, etc.)
   - Percentages to float
   - Dates to datetime
4. Data quality:
   - Remove duplicates at commune level
   - Handle missing values (filled with 0 or NaN as appropriate)
5. Feature engineering:
   - Calculate percentages: (part / total) * 100
   - Handle division by zero with np.where()
6. Aggregate from voting bureau → commune level:
   - Sum votes and metrics by commune
   - Recalculate percentages at commune level
7. Unpivot candidate data (11+ columns → long format rows)
8. Save to SILVER layer
```

**Notebooks:**
- `2017-pre/2017-pres-t1-commune.ipynb` - Clean 2017 T1 data
- `2017-pre/2017-pres-t2-commune.ipynb` - Clean 2017 T2 data
- `2022-pre/2022-pres-t1-commune.ipynb` - Clean 2022 T1 data
- `2022-pre/2022-pres-t2-commune.ipynb` - Clean 2022 T2 data
- `nuance politique/nuance politique.ipynb` - Transform candidate-nuance mappings
- `securite/16-24 security.ipynb` - Clean crime indicator data

**Key Operations:**
- Stratified aggregation (maintain class distribution)
- Candidate column unpivoting (wide → long format)
- INSEE code extraction and formatting (XXXXX → XX-XXX)
- Date parsing and standardization

### 3. Orchestration Stage
**Location:** `src/orchestration/`

**Inputs:** SILVER layer data

**Process:**
```
SILVER data (multiple files for different elections/datasets)
    ↓
1. Load all SILVER CSV files (2017 T1, 2017 T2, 2022 T1, 2022 T2)
2. Concatenate all election data
3. Extract and create dimension tables:
   
   a. dim_election:
      - Unique (year, round) combinations
      - Create id_election as "YYYY_TZ" key
   
   b. dim_commune:
      - Unique (code_dept, libelle_commune) combinations
      - Create id_commune as "DD-CCC" (padded codes)
   
   c. dim_candidat:
      - Unique (nom, prenom, sexe, nuance) combinations
      - Create sequential id_candidat (CAND_001, CAND_002, etc.)
   
   d. dim_indicateur_securite:
      - Unique crime/security indicator types
      - Create id for each indicator type
4. Create fact tables:
   
   a. fact_resultats:
      - Foreign keys: id_commune, id_election, id_candidat
      - Metrics: inscrits, abstentions, voix, percentages, etc.
      - Add date_integration_gold timestamp
   
   b. fact_securite:
      - Foreign keys: id_commune, id_indicateur
      - Metrics: taux_pour_mille, insee_pop, insee_log
      - Filtered to years 2021-2022
5. Export to GOLD layer
```

**Notebooks:**
- `election_build_gold_layer.ipynb` - Build election star schema
- `security_build_gold_layer.ipynb` - Build security star schema

**Key Transformations:**
- Denormalization (normalize → dimensions + facts)
- Surrogate key creation with auto-increment
- Foreign key relationships established
- Multi-table join operations

### 4. ML Model Stage
**Location:** `src/ml/`

**Inputs:** GOLD layer data

**Process:**
```
GOLD tables (dim_* and fact_*)
    ↓
1. Assemble dataset:
   - Load all gold layer tables
   - Extract features from fact_resultats and fact_securite
   - Target: 1 if MACRON votes > LE PEN votes in commune (2022 T2)
   - Target: 0 otherwise
2. Feature engineering:
   - Select relevant columns from both facts tables
   - Handle missing commune data (left join on communes)
3. Data split (stratified):
   - Train: 80% | Test: 20%
   - Maintain class balance (target 0/1 distribution)
4. Data preprocessing:
   - Convert to numeric matrices
   - Impute NaN with column means (using training stats)
   - Standardize features: (value - mean) / std_dev
5. Model training:
   - Logistic regression with gradient descent
   - Parameters:
     * Learning rate (lr): 0.05
     * Epochs: 3000
     * L2 regularization: 0.001
6. Prediction:
   - Generate class predictions (threshold: 0.5)
   - Generate probability estimates for MACRON win
7. Evaluation:
   - Compare vs baseline (majority class predictor)
   - Calculate: accuracy, precision, recall, F1, specificity
8. Output results:
   - model_dataset.csv: Complete feature matrix
   - test_predictions.csv: Predictions on test set
   - feature_importance.csv: Model weights (feature coefficients)
   - metrics.json: Performance metrics
```

**Scripts:**
- `train_poc_model.py` - Main model training pipeline
- `poc_model_walkthrough.ipynb` - Interactive model demonstration

**Two Experiments:**
1. **with_2022_t1**: Includes 2022 T1 election features
2. **without_2022_t1**: Uses only 2017 and security data

## Key Design Decisions

### Why Star Schema?
- **Performance:** Optimized for analytical queries and ML feature extraction
- **Simplicity:** Clear relationships between dimensions and facts
- **Scalability:** Easy to add new dimensions or fact tables
- **Maintainability:** Normalized structure prevents data anomalies

### Why Stratified Split?
- **Balance:** Ensures both train and test sets have same target distribution
- **Fairness:** Prevents bias in imbalanced datasets
- **Reproducibility:** Fixed random seed (42) ensures consistent results

### Why Gradient Descent?
- **Transparency:** Clear parameter tuning (learning rate, epochs)
- **Control:** Direct visibility into optimization process
- **Educational:** Demonstrates ML fundamentals
- **Custom Loss:** Enables L2 regularization for better generalization

### Why Impute with Training Stats?
- **Prevents Leakage:** Test data uses training statistics only
- **Realistic:** Reflects real-world scenario (unknown test data)
- **Robust:** Standardization handles different feature scales

## File Organization Rationale

```
src/
├── ingestion/   ← Data source-specific notebooks
│   └── [one folder per data source, year, or category]
├── transformation/ ← Data cleaning and preparation
│   └── [one folder per data domain]
├── orchestration/  ← Building warehouse structures
└── ml/           ← Analytics and predictions
```

This structure allows:
- **Parallel Development:** Different teams can work on different stages
- **Reusability:** Transformation logic can be reused across notebooks
- **Maintenance:** Changes to one stage don't affect others
- **Testing:** Each stage can be tested independently

## Performance Considerations

### Data Volume
- ~100-1000 communes per election
- ~11-16 candidates per voting bureau
- ~2-8 million rows from raw voting data
- Reduced to ~1000-10000 rows after aggregation to commune level

### Processing Time
- Ingestion: Download ~100MB files (5-30 seconds each)
- Transformation: Aggregation and joins (~30-60 seconds per year)
- Orchestration: Building star schema (~10-20 seconds)
- ML Model: Training logistic regression (0.5-2 seconds)

### Memory Usage
- Notebooks: ~1-2GB RAM sufficient for all data
- Model: Lightweight (< 100KB for trained weights)

## Error Handling Strategy

### Data Validation
- Type checking after conversions
- Range validation for percentages (0-100)
- Uniqueness checks on keys (id_election, id_commune, etc.)

### Missing Data
- Communes in elections but not in security data: Left join (null values)
- Candidates present in some elections but not others: Dictionary lookup with fallback
- Population data missing: Use INSEE fallback values where available

### External Failures
- Download failures: Retry or use cached local copy
- API rate limits: Implemented with timeout (180 seconds)
- Encoding issues: Handle multiple encodings (UTF-8, cp1252)

## Future Architecture Enhancements

1. **Incremental Updates:** Change Data Capture (CDC) for new elections
2. **Real-time Processing:** Stream election results as they arrive
3. **Data Versioning:** Track schema and data changes over time
4. **Metadata Management:** Catalog of all data assets
5. **Quality Metrics:** Automated data quality checks
6. **Dashboard Layer:** BI tool integration for visualization
7. **API Layer:** REST API for model predictions
8. **Containerization:** Docker deployment for reproducibility

---

**Last Updated:** April 2026
**Architecture Pattern:** Medallion (Bronze-Silver-Gold)
**Data Warehouse Schema:** Star Schema
**ML Framework:** Logistic Regression (Pure Python implementation)
