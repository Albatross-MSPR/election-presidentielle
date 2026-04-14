# Data Dictionary

This document defines all columns, codes, and data structures used throughout the project across the medallion architecture layers.

## Table of Contents
1. [Election Data Structure](#election-data-structure)
2. [Political Nuances (Tendances Politiques)](#political-nuances-tendances-politiques)
3. [INSEE Reference Data](#insee-reference-data)
4. [Security Indicators](#security-indicators)
5. [Gold Layer Star Schema](#gold-layer-star-schema)
6. [Calculated Metrics](#calculated-metrics)

---

## Election Data Structure

### Raw Election Data Columns (BRONZE/SILVER)

#### Common Election Result Columns

| Column Name | Data Type | Description | Example | Layer |
|------------|-----------|-------------|---------|-------|
| `Code du département` | String (2) | ISO 3166-2 French department code | "69" (Rhône) | Bronze/Silver |
| `Libellé du département` | String | Department full name | "Rhône" | Bronze/Silver |
| `Code de la commune` | String (5) | INSEE commune code (2-digit dept + 3-digit commune) | "69266" (Villeurbanne) | Bronze/Silver |
| `Libellé de la commune` | String | Commune full name | "Villeurbanne" | Bronze/Silver |
| `Inscrits` | Integer | Registered voters | 45230 | Bronze/Silver |
| `Abstentions` | Integer | Number of abstentions | 12450 | Bronze/Silver |
| `Votants` | Integer | Number who voted (Inscrits - Abstentions) | 32780 | Bronze/Silver |
| `Blancs` | Integer | Blank votes (no valid candidate selected) | 156 | Bronze/Silver |
| `Nuls` | Integer | Void/spoiled votes (invalid) | 248 | Bronze/Silver |
| `Exprimés` | Integer | Valid expressed votes (Votants - Blancs - Nuls) | 32376 | Bronze/Silver |

#### Candidate Result Columns (Dynamic)

Election data contains candidate columns. The number of candidates varies (typically 11-16).

**Column Pattern:** `Voix de [FIRST_NAME] [LAST_NAME]`

Example columns:
- `Voix de Bruno Leroy` - PPP candidate votes
- `Voix de Eva Sas` - LREM candidate votes  
- `Voix de Marine Le Pen` - RN candidate votes
- `Voix de Jean-Luc Mélenchon` - LFI candidate votes

**Data Type:** Integer (number of votes received)

#### Additional Columns

| Column Name | Data Type | Description | Values |
|------------|-----------|-------------|--------|
| `% Abstention` | Float | Abstention percentage (Abstentions/Inscrits)*100 | 0.0-100.0 |
| `Autres` | Integer | Votes for candidates not listed individually | 0-1000 |
| `Dont Étranger (Inscrits)` | Integer | Foreign nationals registered to vote | 0-5000 |
| `Dont Étranger (Abstentions)` | Integer | Foreign nationals who abstained | 0-5000 |

---

## Political Nuances (Tendances Politiques)

French political spectrum is categorized by nuance codes representing candidate political orientation:

### Nuance Code Reference

| Code | Full Name | English | Position | Characteristics |
|------|-----------|---------|----------|-----------------|
| **EXG** | Extrême Gauche | Far Left | ← ← ← | Communists, radical socialists |
| **FI** | France Insoumise | Unbowed France | ← ← | Jean-Luc Mélenchon's La France Insoumise |
| **SOC** | Socialistes | Socialists | ← | Socialist Party (PS) |
| **RAD** | Radicaux | Radicals | ← | Rural, regionalist left |
| **DVG** | Divers Gauche | Miscellaneous Left | ← | Independent left candidates |
| **LREM** | La République En Marche | Republic On The Move | → → | Emmanuel Macron's centrist party |
| **MODEM** | Mouvement Démocrate | Democratic Movement | → | François Bayrou's centrist party |
| **LR** | Les Républicains | The Republicans | → → → | Conservative right (Sarkozy's party) |
| **DLF** | Debout la France | France Standing | → → | Patriotic right (Dupont-Aignan) |
| **RN** | Rassemblement National | National Rally | → → → → | Marine Le Pen's far-right party (formerly FN) |
| **DVR** | Divers Droite | Miscellaneous Right | → → | Independent right candidates |
| **PRG** | Parti Radical de Gauche | Radical Left Party | ← | Small left-wing party |
| **UDI** | Union des Démocrates Indépendants | Union of Independent Democrats | → | Centrist party (Bayrou) |
| **Éco** | Écologistes | Greens/Ecologists | ← → | Les Verts (ecological party) |

### Spectrum Visualization

```
EXG - FI - SOC - RAD - DVG - LREM/MODEM - LR - DLF - RN
←←← ←← ← ← ← ↔ → → → → →
FAR LEFT         CENTER      CENTER-RIGHT    FAR RIGHT
```

### Key Context

- **LREM (La République En Marche):** Emmanuel Macron's centrist party, represents moderate reformism
- **RN (Rassemblement National):** Marine Le Pen's party, historically FN (Front National), represents far-right nationalism
- **FI (France Insoumise):** Jean-Luc Mélenchon's party, represents radical left
- **LR (Les Républicains):** French conservative/right party

---

## INSEE Reference Data

INSEE = Institut National de la Statistique et des Études Économiques (National Institute of Statistics)

### INSEE Commune Code Structure

**Format:** `XXXXX` (5 digits)

**Breakdown:**
- **XX** (First 2 digits): Department code (département)
- **XXX** (Last 3 digits): Commune code (commune)

**Examples:**
| Code | Department | Commune |
|------|------------|---------|
| `69000` | 69 (Rhône) | 000 (Lyon) |
| `69123` | 69 (Rhône) | 123 |
| `69266` | 69 (Rhône) | 266 (Villeurbanne) |
| `75056` | 75 (Paris) | 056 (Paris) |

### Department Codes (Departments in Project)

| Code | Department | Region | Notes |
|------|-----------|--------|-------|
| **69** | Rhône | Auvergne-Rhône-Alpes | Primary focus of this project |

### Population Data (INSEE)

| Column | Data Type | Description | Range | Source |
|--------|-----------|-------------|-------|--------|
| `insee_pop` | Integer | INSEE resident population | 50-500000 | INSEE Census data |
| `insee_log` | Integer | INSEE total housing units | 20-200000 | INSEE Housing Census |

---

## Security Indicators

### Crime Data Fields

#### Core Security Metrics

| Column Name | Data Type | Description | Units | Notes |
|------------|-----------|-------------|-------|-------|
| `Annee` | Integer | Year of measurement | 2021-2022 | Range of data |
| `Code_Commune` | String (5) | INSEE commune code | "69000" | Numeric string |
| `Indicateur` | String | Type of crime/security metric | See below | Security category |
| `Nombre_Faits` | Integer | Number of incidents reported | 0-10000 | Varies by size |
| `Taux_Pour_Mille` | Float | Incidents per 1000 population | 0.0-100.0 | Normalized to population |

#### Security Indicator Types

Common crime categories tracked:

| Indicateur Code | Indicateur Name | Category | Description |
|-----------------|-----------------|----------|-------------|
| `VIOLENCES` | Violences | Violent Crime | Homicides, assaults, aggravated battery |
| `CAMBRIOLAGES` | Cambriolages | Property Crime | Burglaries and break-ins |
| `VOLS` | Vols | Theft | Robbery and theft incidents |
| `DROGUE` | Trafic de Drogue | Drug Crime | Drug trafficking and possession |
| `FRAUDE` | Fraude | Financial Crime | Fraud and forgery |
| `CONDUITES` | Conduites Dangereuses | Traffic | Dangerous driving and DUI |

### Population Adjustment

The `taux_pour_mille` (rate per thousand) is calculated to normalize for population differences:

```
taux_pour_mille = (Nombre_Faits / insee_pop) * 1000
```

This allows comparison between small villages and large cities.

---

## Gold Layer Star Schema

### Dimension Tables

#### `dim_election`

Election dimension - temporal structure of elections

| Column | PK | Data Type | Description |
|--------|----|---------|-------------|
| `id_election` | ✓ | String | Unique identifier "YYYY_TZ" (e.g. "2022_T2") |
| `annee` | | Integer | Election year (2017, 2022) |
| `tour` | | String | Election round (T1 = First round, T2 = Second round) |
| `date_election` | | Date | Election date (derived from year/tour) |

**Example Data:**
```
id_election | annee | tour | date_election
2017_T1     | 2017  | T1   | 2017-04-23
2017_T2     | 2017  | T2   | 2017-05-07
2022_T1     | 2022  | T1   | 2022-04-10
2022_T2     | 2022  | T2   | 2022-04-24
```

#### `dim_commune`

Commune dimension - geographical structure

| Column | PK | Data Type | Description |
|--------|----|---------|-------------|
| `id_commune` | ✓ | String | Commune ID "DD-CCC" padded (e.g. "69-000") |
| `code_insee` | | String | INSEE code unpadded (e.g. "69000") |
| `code_dept` | | String | 2-digit department code (e.g. "69") |
| `libelle_commune` | | String | Commune name (e.g. "Lyon") |

**Example Data:**
```
id_commune | code_insee | code_dept | libelle_commune
69-000     | 69000      | 69        | Lyon
69-123     | 69123      | 69        | Villeurbanne
69-266     | 69266      | 69        | Saint-Fons
```

#### `dim_candidat`

Candidate dimension - all candidates across all elections

| Column | PK | Data Type | Description |
|--------|----|---------|-------------|
| `id_candidat` | ✓ | Integer | Auto-generated sequential ID (CAND_001, etc.) |
| `nom` | | String | Candidate last name |
| `prenom` | | String | Candidate first name |
| `sexe` | | Char(1) | Gender (M/F) |
| `nuance` | | String | Political nuance code (see Political Nuances section) |

**Example Data:**
```
id_candidat | nom       | prenom  | sexe | nuance
1           | MACRON    | Emmanuel| M    | LREM
2           | LE PEN    | Marine  | F    | RN
3           | MÉLENCHON | Jean-Luc| M    | FI
4           | DUPONT    | François| M    | DLF
```

#### `dim_indicateur_securite`

Security indicator dimension - types of crime tracked

| Column | PK | Data Type | Description |
|--------|----|---------|-------------|
| `id_indicateur_securite` | ✓ | Integer | Auto-generated ID |
| `indicateur` | | String | Crime/security category code (VIOLENCES, VOLS, etc.) |
| `libelle_indicateur` | | String | Full indicator name |

**Example Data:**
```
id_indicateur_securite | indicateur  | libelle_indicateur
1                      | VIOLENCES   | Violences
2                      | CAMBRIOLAGES| Cambriolages
3                      | VOLS        | Vols
```

### Fact Tables

#### `fact_resultats`

Electoral results - aggregated by commune and election

| Column | FK | Data Type | Description |
|--------|----|---------|-| 
| `id_commune` | ✓ | String | Foreign key → dim_commune.id_commune |
| `id_election` | ✓ | String | Foreign key → dim_election.id_election |
| `id_candidat` | ✓ | Integer | Foreign key → dim_candidat.id_candidat |
| `inscrits` | | Integer | Registered voters |
| `abstentions` | | Integer | Number of abstentions |
| `votants` | | Integer | People who voted |
| `blancs` | | Integer | Blank votes |
| `nuls` | | Integer | Spoiled/void votes |
| `exprimes` | | Integer | Valid votes expressed |
| `voix_candidat` | | Integer | Votes for this candidate |
| `pct_inscrits` | | Float | (voix_candidat / inscrits) × 100 |
| `pct_exprimés` | | Float | (voix_candidat / exprimes) × 100 |
| `date_integration_gold` | | DateTime | When row was loaded into gold layer |

**Example Data:**
```
id_commune | id_election | id_candidat | inscrits | voix_candidat | pct_inscrits | pct_exprimés
69-000     | 2022_T2     | 1 (MACRON) | 35000    | 14210         | 40.6%        | 42.1%
69-000     | 2022_T2     | 2 (LE PEN) | 35000    | 10850         | 31.0%        | 32.1%
```

#### `fact_securite`

Security metrics - crime indicators by commune and year

| Column | FK | Data Type | Description |
|--------|----|---------|-------------|
| `id_commune` | ✓ | String | Foreign key → dim_commune.id_commune |
| `id_indicateur_securite` | ✓ | Integer | Foreign key → dim_indicateur_securite.id_indicateur_securite |
| `annee` | | Integer | Year (2021, 2022) |
| `nombre_faits` | | Integer | Number of incidents reported |
| `taux_pour_mille` | | Float | Incidents per 1000 population |
| `insee_pop` | | Integer | Population (from INSEE) |
| `insee_log` | | Integer | Total housing units (from INSEE) |
| `date_integration_gold` | | DateTime | When row was loaded into gold layer |

**Example Data:**
```
id_commune | id_indicateur_securite | annee | nombre_faits | taux_pour_mille | insee_pop
69-000     | 1 (VIOLENCES)         | 2022  | 156          | 18.2            | 8560
69-000     | 2 (CAMBRIOLAGES)      | 2022  | 42           | 4.9             | 8560
```

---

## Calculated Metrics

### Voter Turnout Metrics

```
Abstention Rate (%) = (Abstentions / Inscrits) × 100
Voting Rate (%) = (Votants / Inscrits) × 100
Blank Vote Rate (%) = (Blancs / Inscrits) × 100
Void Vote Rate (%) = (Nuls / Inscrits) × 100
```

### Candidate Performance Metrics

```
Candidate % of Registered = (Voix_Candidat / Inscrits) × 100
Candidate % of Expressed = (Voix_Candidat / Exprimes) × 100
Vote Share vs Main Competitor = Voix_Candidat / Voix_Competitor
```

### Macro Predictions

The ML model predicts: **Does candidate X win in commune Y?**

```
Target Variable (2022 T2):
  1 if MACRON_VOTES > LE_PEN_VOTES in commune
  0 if LE_PEN_VOTES >= MACRON_VOTES in commune
```

### Data Quality Metrics

**Validation Checks Applied:**

```
✓ Percentage values must be 0-100
✓ inscrits ≥ votants ≥ exprimes (voter cascade)
✓ Total candidat votes ≤ exprimes (vote ceiling)
✓ All negative values trigger warnings
✓ Population values > 0 (except sparse communes may have 0)
```

---

## Data Types Reference

### Python/Pandas Type Mapping

| Logical Type | Python Type | Pandas Dtype | SQLite Equivalent |
|-------------|-------------|--------------|-------------------|
| Identifier | String | object | TEXT |
| Count | Integer | int64 | INTEGER |
| Percentage | Float | float64 | REAL |
| Rate | Float | float64 | REAL |
| Category | String | object | TEXT |
| Date | DateTime | datetime64 | DATE |
| Name | String | object | TEXT |

### Null Handling Strategy

| Scenario | Handling | Reason |
|----------|----------|--------|
| Missing commune in security data | Left join with NULL | Preserve all election data |
| Missing candidate nuance | Default to "DVR"/"DVG" | Unknown political orientation |
| Missing population (insee_pop) | Use median commune size | Enable rate calculations |
| Division by zero (0 population) | Return 0.0 instead of inf | Prevent numerical errors |

---

## Examples of Data Extraction

### Example 1: Getting Macron's 2022 T2 Results for Lyon

```sql
SELECT 
    c.libelle_commune,
    r.inscrits,
    r.voix_candidat as votes_macron,
    r.pct_exprimés,
    r.pct_inscrits
FROM fact_resultats r
JOIN dim_commune c ON r.id_commune = c.id_commune
JOIN dim_candidat cand ON r.id_candidat = cand.id_candidat
JOIN dim_election e ON r.id_election = e.id_election
WHERE 
    c.libelle_commune = 'Lyon'
    AND e.id_election = '2022_T2'
    AND cand.nom = 'MACRON'
```

### Example 2: Security Trends by Commune

```sql
SELECT 
    c.libelle_commune,
    s.annee,
    i.libelle_indicateur,
    s.taux_pour_mille,
    s.nombre_faits
FROM fact_securite s
JOIN dim_commune c ON s.id_commune = c.id_commune
JOIN dim_indicateur_securite i ON s.id_indicateur_securite = i.id_indicateur_securite
WHERE c.code_dept = '69'
ORDER BY c.libelle_commune, s.annee
```

### Example 3: Finding High-Vote-Share Candidates

```sql
SELECT 
    cand.nom,
    cand.prenom,
    cand.nuance,
    COUNT(*) as num_communes,
    AVG(r.pct_exprimés) as avg_pct_expressed
FROM fact_resultats r
JOIN dim_candidat cand ON r.id_candidat = cand.id_candidat
WHERE r.pct_exprimés >= 20.0
GROUP BY cand.id_candidat
ORDER BY avg_pct_expressed DESC
```

---

## Data Entry/Collection Standards

### For New Elections

When adding a new election year:
1. Use existing column names and patterns
2. Add year to dim_election
3. Map all candidates to existing or new nuance codes
4. Follow same aggregation logic
5. Add date_integration_gold timestamp

### For New Security Indicators

When adding crime data:
1. Create entry in dim_indicateur_securite
2. Use consistentIndicateur code naming (e.g., `INDICATRICE_NAME`)
3. Calculate taux_pour_mille using formula with insee_pop
4. Include year in data (2021-2022 minimum)

---

**Last Updated:** April 2026
**Version:** 1.0
**Project:** French Presidential Election Analysis
