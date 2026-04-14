# French Presidential Election Analysis Project

## Overview

This project analyzes French presidential election results for the Rhône region (department 69), combining electoral data from 2017 and 2022 with security indicators to build predictive models. The goal is to understand electoral patterns and predict electoral outcomes based on historical data and socioeconomic factors.

**Target Variable:** Predicting whether Emmanuel Macron will win in a given commune during the 2022 presidential election second round.

## Project Structure

```
election-presidentielle/
├── data/                          # Data storage (organized by processing layer)
│   ├── raw/                       # Raw unprocessed data from sources
│   ├── bronze/                    # Initial ingestion (raw-ish, readable format)
│   ├── silver/                    # Cleaned, transformed data
│   └── gold/                      # Final aggregated data (star schema)
│       ├── election/              # Electoral dimensions and facts
│       └── security/              # Security indicator dimensions and facts
├── src/                           # Source code organized by processing stage
│   ├── ingestion/                 # Data ingestion from external sources
│   │   ├── 2017_pres_t1/
│   │   ├── 2017_pres_t2/
│   │   ├── 2022_pres_t1/
│   │   ├── 2022_pres_t2/
│   │   ├── DEP-departementale/
│   │   ├── ListeDesCommunes/
│   │   └── nuance politique/
│   ├── transformation/            # Data cleaning and transformation
│   │   ├── 2017-pre/
│   │   ├── 2022-pre/
│   │   ├── nuance politique/
│   │   └── securite/
│   ├── orchestration/             # Building gold layer star schema
│   └── ml/                        # Machine learning models
│       ├── train_poc_model.py     # Model training pipeline
│       ├── poc_model_walkthrough.ipynb
│       └── outputs/               # Model results and predictions
├── config/                        # Configuration files
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Data Pipeline Architecture

### Processing Layers (Medallion Architecture)

The project follows a **medallion data architecture** with three layers:

1. **Raw Layer** - Original source files as downloaded
   - Unmodified data from external APIs
   - Various formats (CSV, TXT)

2. **Bronze Layer** - Initial ingestion
   - Minimal transformations
   - Audit columns added (source URL, ingestion timestamp, filename)
   - Basic data quality checks
   - Still contains all raw columns

3. **Silver Layer** - Cleaned data
   - Column cleaning and standardization
   - Type conversions
   - Data validation
   - Duplicate removal
   - Missing value handling
   - Business logic mappings

4. **Gold Layer** - Aggregated business data
   - Star schema design (dimensional + fact tables)
   - Optimized for analytics and ML
   - Ready for consumption

### Data Sources

#### Election Data
- **2017 Presidential Election T1 & T2**: Results by voting bureau and commune
  - Source: data.gouv.fr API
  - Files: PR17_BVot_T1_FE.txt, PR17_BVot_T2_FE.txt

- **2022 Presidential Election T1 & T2**: Results by voting bureau and commune
  - Source: data.gouv.fr API
  - Multiple candidates per row (up to 16 candidates per voting bureau)

#### Reference Data
- **Liste des Communes**: Commune reference list for Rhône
- **Nuances Politiques**: Political spectrum code dictionary (mapping candidate names to political orientations)

#### Security Data
- **Crime Indicators (2021-2022)**: Public security statistics by commune
  - Crime rates normalized per 1000 inhabitants
  - INSEE population and housing data
  - Prefecture of Police data

## Quick Start

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)
- Git (for source control)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd election-presidentielle
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\Activate.ps1
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

#### Full Pipeline
Run notebooks in order:

1. **Ingestion** (Extract from sources):
   ```
   src/ingestion/2017_pres_t1/2017-pres-t1-commune.ipynb
   src/ingestion/2017_pres_t2/2017-pres-t2-commune.ipynb
   src/ingestion/2022_pres_t1/2022-pres-t1-commune.ipynb
   src/ingestion/2022_pres_t2/2022-pres-t2-commune.ipynb
   src/ingestion/nuance politique/nuance politique.ipynb
   src/ingestion/securite/...
   ```

2. **Transformation** (Clean & prepare):
   ```
   src/transformation/2017-pre/
   src/transformation/2022-pre/
   src/transformation/nuance politique/
   src/transformation/securite/
   ```

3. **Orchestration** (Build gold layer):
   ```
   src/orchestration/election_build_gold_layer.ipynb
   src/orchestration/security_build_gold_layer.ipynb
   ```

4. **ML Model** (Train & predict):
   ```
   python src/ml/train_poc_model.py
   ```

#### Expected Output

After running the full pipeline:
```
data/gold/election/
├── dim_election.csv       # Dimensions: elections (year, round)
├── dim_commune.csv        # Dimensions: communes
├── dim_candidat.csv       # Dimensions: candidates
└── fact_resultats.csv     # Facts: election results

data/gold/security/
├── dim_indicateur_securite.csv
└── fact_securite.csv

src/ml/outputs/
├── with_2022_t1/          # Model results including 2022 T1 features
│   ├── model_dataset.csv
│   ├── test_predictions.csv
│   ├── feature_importance.csv
│   └── metrics.json
├── without_2022_t1/       # Model results excluding 2022 T1
│   └── (same files as above)
└── comparison.json        # Comparison of both experiments
```

## Data Model

### Star Schema (Gold Layer)

**Dimension Tables:**
- `dim_election`: Electoral events (year, round)
- `dim_commune`: Geographic units (communes, departments)
- `dim_candidat`: Candidates with names and political orientations
- `dim_indicateur_securite`: Security indicator types

**Fact Tables:**
- `fact_resultats`: Electoral results (votes, percentages by commune, candidate, election)
- `fact_securite`: Security indicators (crime rates by indicator, commune, year)

### Key Relationships
```
fact_resultats ──→ dim_election
fact_resultats ──→ dim_commune
fact_resultats ──→ dim_candidat

fact_securite ──→ dim_indicateur_securite
fact_securite ──→ dim_commune
```

## Machine Learning Model

### Model Type
Binary logistic regression trained with gradient descent and L2 regularization.

### Target Variable
```
1 if Macron wins the commune in 2022 T2 election
0 if Le Pen wins the commune in 2022 T2 election
```

### Features
Elections features from 2017, 2022 T1 (optional), and security indicators:
- Voter participation metrics (registered, abstentions, voters, blanks, nulls, expressed)
- Candidate vote shares per election
- Crime rate indicators (normalized per 1000 inhabitants)
- INSEE population and housing data

### Experiments

**Experiment 1: with_2022_t1**
- Includes 2022 first round data as features
- Tests if recent election results are predictive

**Experiment 2: without_2022_t1**
- Uses only 2017 and security data
- Tests model without 2022 T1 information

### Results Location
```
src/ml/outputs/comparison.json  # Overall model comparison
```

## Key Files Explained

### Python Module
- **`src/ml/train_poc_model.py`**: 
  - Implements data loading, preprocessing, model training
  - Functions for metrics calculation, stratified splitting, prediction
  - Fully documented with docstrings for each function

### Configuration
- **`config/config.yaml`**: Configuration parameters (currently empty, can be populated)

### Documentation
- **`README.md`**: This file - project overview and setup
- **`DATA_DICTIONARY.md`**: Complete column and code definitions (see separate file)
- **`ARCHITECTURE.md`**: Detailed data flow and system design (see separate file)

## Dependencies

See `requirements.txt` for complete list:
- pandas: Data manipulation and analysis
- numpy: Numerical computations
- requests: HTTP requests for data download
- jupyter: Interactive notebooks
- matplotlib/seaborn: Data visualization (optional)

## Common Tasks

### Rerun a specific notebook
```bash
jupyter notebook src/ingestion/2017_pres_t1/2017-pres-t1-commune.ipynb
```

### Check gold layer data
```bash
cd data/gold/election
ls -la
head -5 dim_candidat.csv
```

### Modify model parameters
Edit in `src/ml/train_poc_model.py`:
- `lr` (learning rate): default 0.05
- `epochs`: default 3000
- `l2` (regularization): default 0.001
- `test_size`: default 0.2 (20% test set)

## Troubleshooting

### Data download fails
- Check internet connection
- Verify data.gouv.fr API is accessible
- Check URL format in source notebooks

### Memory issues
- Notebooks load entire datasets into memory
- Consider filtering data before loading for large datasets

### Model not converging
- Adjust learning rate (`lr` parameter)
- Increase number of epochs
- Check feature normalization in preprocessing

## Project Team & Attribution

This is an election analysis project for the Rhône region, combining:
- Historical electoral data (2017, 2022)
- Security/crime indicators
- INSEE socioeconomic data

## License

[Specify your license here]

## Contact & Support

For questions or issues, please refer to project documentation or contact the development team.

---

**Last Updated:** April 2026
**Status:** Active
**Version:** 1.0
