# Guide d'exécution — Pipeline Electio Analytics

Ce guide décrit comment exécuter le pipeline de bout en bout, reconstruire les zones de données, lancer les contrôles qualité et mettre à jour le dashboard.

---

## Prérequis

```bash
# Cloner le dépôt
git clone https://github.com/Albatross-MSPR/election-presidentielle.git electio-analytics-poc
cd electio-analytics-poc

# Créer et activer l'environnement virtuel
python3 -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows

# Installer les dépendances
pip install -r requirements.txt
```

**Versions requises :** Python ≥ 3.10 · pandas 2.2.3 · scikit-learn 1.8.0 · DuckDB 1.5.1 · JupyterLab 4.4.0

> Les commandes ci-dessous supposent que l'environnement virtuel est activé. Utilisez `python`, pas `python3`, pour éviter d'appeler un interpréteur système différent de `.venv`.

---

## Orchestrateur principal — `run_pipeline.py`

Fichier : `src/orchestration/run_pipeline.py`

### Commandes

```bash
# Pipeline complet : ingestion → transformation → gold → qualité
python src/orchestration/run_pipeline.py

# Reprendre à partir de la transformation (bronze déjà présent)
python src/orchestration/run_pipeline.py --start-from transformation

# Reconstruire uniquement le gold et lancer la qualité
python src/orchestration/run_pipeline.py --stages gold quality

# Reconstruire uniquement le gold (sans qualité)
python src/orchestration/run_pipeline.py --stages gold

# Voir le plan d'exécution sans rien exécuter
python src/orchestration/run_pipeline.py --dry-run

# Lister tous les notebooks et leurs I/O
python src/orchestration/run_pipeline.py --list
```

Le runner sélectionne maintenant automatiquement le kernelspec Jupyter pointant vers `.venv/bin/python` et l'affiche dans le log (`Kernel : ...`).

### Codes de sortie

| Code | Signification |
|------|---------------|
| `0` | Toutes les étapes ont réussi |
| `1` | Au moins un notebook ou contrôle qualité a échoué |

> `run_pipeline.py` ne renvoie actuellement que `0` ou `1`. Les erreurs de configuration au démarrage (par exemple racine du projet introuvable) lèvent une exception Python avant la boucle pipeline.

### Sorties du pipeline

- **`data/pipeline_logs/<timestamp>_pipeline.log`** — log horodaté complet
- **`data/pipeline_logs/<timestamp>_pipeline.json`** — résumé machine-readable (statut par étape, durées, erreurs)

---

## Les 4 stages en détail

### Stage 1 — Ingestion (`--stages ingestion`)

Lit les fichiers RAW locaux et produit les CSV Bronze. Dans l'état actuel de `run_pipeline.py`, seul `ingest_filosofi` est réellement sans prérequis local ; les autres étapes d'ingestion exigent que les fichiers RAW listés ci-dessous soient déjà présents.

| Clé | Notebook | Prérequis | Sortie Bronze |
|-----|----------|-----------|---------------|
| `ingest_nuance` | `ingestion/nuance politique/dictionnaire des nuances politiques.ipynb` | `data/raw/dictionnaire des nuances politiques.csv` | `bronze_dictionnaire_des_nuances_politiques.csv` |
| `ingest_2012` | `ingestion/2012_pres t1-2/2012-pres-t1-2-commune.ipynb` | `data/raw/2012_pres_t1_t2_communes_france.xls` | bronze 2012 T1 + T2 |
| `ingest_2017_t1` | `ingestion/2017_pres t1/2017-pres-t1-commune.ipynb` | `data/raw/PR17_BVot_T1_FE.txt` | bronze 2017 T1 |
| `ingest_2017_t2` | `ingestion/2017_pres t2/2017-pres-t2-commune.ipynb` | `data/raw/PR17_BVot_T2_FE.txt` | bronze 2017 T2 |
| `ingest_2022_t1` | `ingestion/2022_pres t1/2022-pres-t1-commune.ipynb` | `data/raw/2022_burvot_t1_france_entiere.xlsx` | bronze 2022 T1 |
| `ingest_2022_t2` | `ingestion/2022_pres t2/2022-pres-t2-commune.ipynb` | `data/raw/resultats-par-niveau-subcom-t2-france-entiere.xlsx` | bronze 2022 T2 |
| `ingest_dep` | `ingestion/DEP-departementale/DEP-departementale.ipynb` | `data/raw/DEP-departementale.csv` | `bronze_DEP-departementale.csv` |
| `ingest_filosofi` 🌐 | `ingestion/filosofi-14-21/filosofi-14-21.ipynb` | *(télécharge depuis INSEE)* | bronze FILOSOFI ×8 ans |

> `ingest_filosofi` nécessite une connexion internet. Les ZIP sont téléchargés depuis INSEE puis filtrés sur le Rhône (code commune commençant par "69").

> Important : certains notebooks d'ingestion (2012, 2017, 2022, DEP, dictionnaire des nuances) savent télécharger leurs sources lorsqu'ils sont exécutés seuls, mais l'orchestrateur vérifie les prérequis avant exécution. En pratique, avec `run_pipeline.py`, ces fichiers doivent donc être placés dans `data/raw/` en amont.

---

### Stage 2 — Transformation (`--stages transformation`)

Nettoyage et normalisation Bronze → Silver.

| Clé | Notebook | Sortie Silver |
|-----|----------|---------------|
| `transform_nuance` | `transformation/nuance politique/dictionnaire des nuances politiques.ipynb` | `dictionnaire_des_nuances_politiques_silver.csv` |
| `transform_2012_t1` | `transformation/2012-pre/2012-pres-t1-commune.ipynb` | `2012-pres-t1-commune-rhone-69-silver.csv` |
| `transform_2012_t2` | `transformation/2012-pre/2012-pres-t2-commune.ipynb` | `2012-pres-t2-commune-rhone-69-silver.csv` |
| `transform_2017_t1` | `transformation/2017-pre/2017-pres-t1-commune.ipynb` | `2017-pres-t1-commune-rhone-69-silver.csv` |
| `transform_2017_t2` | `transformation/2017-pre/2017-pres-t2-commune.ipynb` | `2017-pres-t2-commune-rhone-69-silver.csv` |
| `transform_2022_t1` | `transformation/2022-pre/2022-pres-t1-commune.ipynb` | `2022-pres-t1-commune-rhone-69-silver.csv` |
| `transform_2022_t2` | `transformation/2022-pre/2022-pres-t2-commune.ipynb` | `2022-pres-t2-commune-rhone-69-silver.csv` |
| `transform_dep` | `transformation/DEP-departementale/DEP-departementale.ipynb` | `silver_DEP-departementale.csv` |
| `transform_filosofi` | `transformation/filosofi/filosofi_bronze_to_silver.ipynb` | `filosofi_2014_2021_commune_silver.csv` |
| `transform_securite` | `transformation/securite/16-24-security.ipynb` | `securite_data_silver_clean.csv` |

> Deux entrées hors pipeline complet restent nécessaires :
> - `data/reference/nuance_politique_candidates_master.csv` pour les transforms élections
> - `data/silver/RAYAN securite_data_silver.csv` pour `transform_securite`

---

### Stage 3 — Gold (`--stages gold`)

Construction du schéma en étoile Silver → Gold. Les notebooks gold écrivent les CSV dans `data/gold/` ; le chargement de `data/electio.duckdb` est une étape séparée.

| Clé | Notebook | Sorties Gold |
|-----|----------|--------------|
| `gold_election` | `orchestration/election_build_gold_layer.ipynb` | `data/gold/election/` ×6 tables |
| `gold_filosofi` | `orchestration/filosofi_build_gold_layer.ipynb` | `data/gold/filosofi/` ×6 tables |
| `gold_security` | `orchestration/securoty-merged.ipynb` | `data/gold/security/` ×6 tables |

---

### Stage 4 — Qualité (`--stages quality`)

Exécute `src/quality/run_quality.py` — 154 règles en 7 catégories.

```bash
# Lancer uniquement les contrôles qualité
python src/quality/run_quality.py

# Par domaine
python src/quality/run_quality.py --domain election
python src/quality/run_quality.py --domain security
python src/quality/run_quality.py --domain filosofi
python src/quality/run_quality.py --domain cross

# Rapport dans un répertoire personnalisé
python src/quality/run_quality.py --output /tmp/quality_reports
```

**Rapports générés dans `data/quality_reports/` :**
- `<timestamp>_quality_report.json` — détail machine-readable complet (154 règles)
- `<timestamp>_quality_report.md` — rapport humain avec tableaux et listes de violations

**Résultat actuel du moteur qualité :** 152/154 PASS (98.7%), 1 CRITICAL (`dim_commune` du domaine élections contient 12 doublons d'`id_commune`), 1 WARNING (couverture commune × élection à 95.6%, soit `1680/1758` paires attendues).

---

## Mise à jour du dashboard

Le dashboard est un fichier HTML statique servi via GitHub Pages. Pour le mettre à jour après une modification des données gold ou des modèles ML :

```bash
# Régénérer les JSON de données
python src/dashboard/export_data.py

# Tester localement (puis ouvrir http://localhost:8080)
python src/dashboard/serve.py
```

Le déploiement sur GitHub Pages est automatique via `.github/workflows/deploy-dashboard.yml` à chaque push sur `main` touchant `src/dashboard/`, `data/gold/` ou `src/ml/**/outputs/`.

---

## Résolution de la racine du projet

Les notebooks et scripts ne résolvent pas tous la racine du projet exactement de la même façon :

```python
# Notebooks
PROJECT_ROOT = Path.cwd().resolve()
while not (PROJECT_ROOT / "data").exists() and PROJECT_ROOT != PROJECT_ROOT.parent:
    PROJECT_ROOT = PROJECT_ROOT.parent

# Scripts Python (run_pipeline.py, run_quality.py, export_data.py)
root = pathlib.Path(__file__).resolve().parent
while root != root.parent and not (root / "data" / "gold").is_dir():
    root = root.parent
```

Les notebooks montent jusqu'au premier dossier contenant `data/`. `run_quality.py` et `run_pipeline.py` s'ancrent aujourd'hui sur `data/gold/`, ce qui fonctionne dans un dépôt déjà construit mais n'est pas un bootstrap universel sur dépôt vide.

---

## Résolution des problèmes courants

### `ModuleNotFoundError: No module named 'pandas'`

L'environnement virtuel n'est pas activé, ou le mauvais Python est utilisé.

```bash
source .venv/bin/activate
python src/orchestration/run_pipeline.py
```

### `ModuleNotFoundError: No module named 'xlrd'` ou mauvais kernel Jupyter

L'orchestrateur exécute les notebooks via Jupyter. Vérifiez que le log affiche un kernel lié au projet, par exemple `Kernel : electio-analytics-poc`, et relancez le pipeline avec l'interpréteur du venv :

```bash
python src/orchestration/run_pipeline.py --dry-run
```

### Un notebook échoue avec `PREREQ_MISSING`

Un fichier d'entrée est manquant. Exemple : les fichiers RAW d'élections ne sont pas dans le dépôt git (trop volumineux). Téléchargez-les manuellement depuis data.gouv.fr et placez-les dans `data/raw/` avec les noms exacts indiqués dans la section Stage 1.

### `jupyter not found`

```bash
pip install jupyterlab nbconvert
```

### Reconstruire DuckDB depuis zéro

```bash
# Supprimer la base si nécessaire
rm data/electio.duckdb
```

DuckDB n'est pas recréé par `run_pipeline.py`. Pour le reconstruire, exécutez `createDB.ipynb` (chargement explicite des CSV gold vers `data/electio.duckdb`) ou relancez les notebooks ML qui contiennent eux aussi des cellules de chargement depuis les CSV gold.

---

## Vue d'ensemble des fichiers de sortie

```
data/
├── bronze/                     CSV Bronze par source / millésime
├── silver/                     CSV Silver normalisés + entrée manuelle sécurité
├── gold/
│   ├── election/               6 CSV   — schéma en étoile élections
│   ├── security/               6 CSV   — schéma en étoile sécurité
│   └── filosofi/               6 CSV   — schéma en étoile revenus/pauvreté
├── electio.duckdb              Entrepôt SQL (chargement séparé)
├── quality_reports/            Rapports qualité horodatés (.json + .md, générés à la demande)
└── pipeline_logs/              Logs et résumés d'exécution pipeline (générés à la demande)

src/
├── ml/commune_model/outputs/   CSV + JSON d'évaluation — scores vulnérabilité, profils, prédictions
├── ml/timeseries/outputs/      CSV de projections abstention 2023–2025
└── dashboard/data/             JSON pré-calculés pour le dashboard
```
