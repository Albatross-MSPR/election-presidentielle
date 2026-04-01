#!/usr/bin/env python3
"""
Export gold layer + ML outputs to JSON for the static dashboard.
Run from the project root: python3 src/dashboard/export_data.py
"""

import json
import os
import re
import sys
import pathlib
import pandas as pd

# Resolve project root (must have data/gold subfolder)
ROOT = pathlib.Path(__file__).resolve().parent
while ROOT != ROOT.parent and not (ROOT / "data" / "gold").is_dir():
    ROOT = ROOT.parent

GOLD_ELEC = ROOT / "data" / "gold" / "election"
GOLD_SEC  = ROOT / "data" / "gold" / "security"
GOLD_FILO = ROOT / "data" / "gold" / "filosofi"
ML_COMMUNE = ROOT / "src" / "ml" / "commune_model" / "outputs"
ML_TS      = ROOT / "src" / "ml" / "timeseries" / "outputs"
OUT        = ROOT / "src" / "dashboard" / "data"
OUT.mkdir(parents=True, exist_ok=True)


def canonicalize_communes(df):
    """
    Keep one canonical row per commune id.

    The election gold layer currently contains duplicate commune ids when the
    commune label changed between election vintages (old vs new commune names).
    The rows appear in chronological load order, so keeping the last occurrence
    preserves the latest available label for the shared INSEE code.
    """
    clean = df.copy()
    clean["id_commune"] = clean["id_commune"].astype(str)
    return (
        clean.drop_duplicates(subset=["id_commune"], keep="last")
             .sort_values("libelle_commune")
             .reset_index(drop=True)
    )


def with_commune_names(df, commune_ref):
    clean = df.copy()
    clean["id_commune"] = clean["id_commune"].astype(str)
    if "libelle_commune" in clean.columns:
        clean = clean.drop(columns=["libelle_commune"])
    return clean.merge(commune_ref, on="id_commune", how="left")

def _clean(obj):
    """Recursively replace NaN/Inf with None so JSON is valid."""
    import math
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(i) for i in obj]
    return obj

def save(name, obj):
    path = OUT / f"{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_clean(obj), f, ensure_ascii=False)
    print(f"  ✓ {name}.json  ({path.stat().st_size // 1024} KB)")

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_clean(obj), f, ensure_ascii=False, indent=2)

def read_gold(filename, domain="election"):
    if domain == "election":
        base = GOLD_ELEC
    elif domain == "security":
        base = GOLD_SEC
    elif domain == "filosofi":
        base = GOLD_FILO
    else:
        raise ValueError(f"Unknown gold domain: {domain}")
    return pd.read_csv(base / filename, sep=";", low_memory=False)

def read_ml(filename, subdir=None):
    base = ML_COMMUNE if subdir is None else ML_TS
    return pd.read_csv(base / filename)

def load_json(path):
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def normalize_commune_ids(series):
    return (
        series.astype(str)
              .str.replace(r"\.0$", "", regex=True)
              .str.strip()
              .str.zfill(5)
    )

def extract_current_notebook_metrics():
    """
    Parse the current commune ML notebook outputs to build dashboard-ready
    metrics for the active nuance classifier.

    This keeps the dashboard aligned with the notebook that actually generated
    the saved ML outputs instead of relying on stale hand-written metrics.
    """
    notebook_path = ROOT / "src" / "ml" / "commune_model" / "commune_ml.ipynb"
    if not notebook_path.exists():
        return {}

    try:
        with open(notebook_path, encoding="utf-8") as f:
            nb = json.load(f)
    except Exception:
        return {}

    output_chunks = []
    for cell in nb.get("cells", []):
        for output in cell.get("outputs", []):
            if isinstance(output, dict) and "text" in output:
                output_chunks.append("".join(output["text"]))
    output_text = "\n".join(output_chunks)

    def _search(pattern):
        match = re.search(pattern, output_text, flags=re.MULTILINE)
        return match.groups() if match else None

    election = _search(r"Using election:\s*([0-9]{4}_T[12])")
    communes = _search(r"Communes with both features and nuance label:\s*(\d+)")
    accuracy = _search(r"^\s*accuracy\s+([0-9.]+)\s+(\d+)\s*$")
    macro = _search(r"^\s*macro avg\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+(\d+)\s*$")
    weighted = _search(r"^\s*weighted avg\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+(\d+)\s*$")
    cv = _search(r"5-fold CV accuracy:\s*([0-9.]+)\s*±\s*([0-9.]+)")
    baseline = _search(r"Baseline \(majority class\):\s*([0-9.]+)")

    if not all([communes, accuracy, macro, weighted, cv]):
        return {}

    latest_election = election[0] if election else None
    latest_election_label = (
        latest_election.replace("_", " ") if latest_election else None
    )

    return {
        "metadata": {
            "description": "Multiclass classification: dominant nuance in the latest notebook election",
            "latest_election": latest_election,
            "latest_election_label": latest_election_label,
            "communes_analyzed": int(communes[0]),
            "test_support": int(accuracy[1]),
            "baseline_accuracy": float(baseline[0]) if baseline else None,
        },
        "current_notebook": {
            "accuracy": float(accuracy[0]),
            "balanced_accuracy": float(macro[1]),
            "macro_f1": float(macro[2]),
            "weighted_f1": float(weighted[2]),
            "cv_accuracy": float(cv[0]),
            "cv_accuracy_std": float(cv[1]),
        },
    }

def build_abstention_evaluation(dim_election, commune_ref):
    """
    Rebuild the abstention regression evaluation from raw gold data using the
    same feature engineering and split/model parameters as the current ML
    notebook. This avoids the corrupted full-sample predictions saved in
    commune_profiles.csv.
    """
    fallback_path = ML_COMMUNE / "abstention_evaluation.json"
    try:
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.metrics import mean_absolute_error, r2_score
        from sklearn.model_selection import cross_val_score, train_test_split
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:
        print(f"  ! Skipping abstention evaluation rebuild: {exc}")
        return load_json(fallback_path)

    try:
        fact_sec = read_gold("fact_securite.csv", "security")
        dim_indic = read_gold("dim_indicateur_securite.csv", "security")
        fact_particip = read_gold("fact_participation.csv")
        fact_menages = read_gold("fact_menages.csv", "filosofi")
        fact_pauvrete = read_gold("fact_pauvrete.csv", "filosofi")
        fact_revenus = read_gold("fact_revenus.csv", "filosofi")
        fact_deciles = read_gold("fact_deciles.csv", "filosofi")

        for df in [
            fact_sec,
            fact_particip,
            fact_menages,
            fact_pauvrete,
            fact_revenus,
            fact_deciles,
        ]:
            df["id_commune"] = normalize_commune_ids(df["id_commune"])

        fact_sec["annee"] = pd.to_numeric(fact_sec["annee"], errors="coerce")
        fact_sec["taux_pour_mille"] = pd.to_numeric(
            fact_sec["taux_pour_mille"], errors="coerce"
        )
        for df in [fact_menages, fact_pauvrete, fact_revenus, fact_deciles]:
            df["id_year"] = pd.to_numeric(df["id_year"], errors="coerce")

        latest_sec_year = int(fact_sec["annee"].max())
        indicator_map = (
            dim_indic.set_index("id_indicateur_securite")["indicateur"].to_dict()
        )
        sec = fact_sec[
            (fact_sec["annee"] == latest_sec_year)
            & (
                fact_sec["est_diffuse"]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin({"true", "1", "diff"})
            )
        ].copy()
        sec["indicateur"] = sec["id_indicateur_securite"].map(indicator_map)
        X_sec = (
            sec.pivot_table(
                index="id_commune",
                columns="indicateur",
                values="taux_pour_mille",
                aggfunc="mean",
            ).apply(lambda c: c.fillna(c.median()))
        )
        X_sec = X_sec.drop(
            columns=[c for c in ["Usage de stupéfiants (AFD)", "Vols avec armes"] if c in X_sec.columns]
        )

        filo = (
            fact_menages.merge(fact_pauvrete, on=["id_commune", "id_year"], how="left")
            .merge(fact_revenus, on=["id_commune", "id_year"], how="left")
            .merge(fact_deciles, on=["id_commune", "id_year"], how="left")
        )
        latest_filo_year = int(filo["id_year"].dropna().max())
        filo = filo[filo["id_year"] == latest_filo_year].copy()
        filo_cols = [c for c in filo.columns if c not in ["id_commune", "id_year"]]
        for col in filo_cols:
            filo[col] = pd.to_numeric(filo[col], errors="coerce")
        X_filo = filo.set_index("id_commune")[filo_cols].add_prefix("fil_")
        drop_filo = [
            "fil_nb_menages_fiscaux",
            "fil_nb_personnes_menages_fiscaux",
            "fil_pct_prestations_familiales",
            "fil_pct_minima_sociaux",
            "fil_pct_prestations_logement",
            "fil_pct_impots",
            "fil_decile_1_revenu",
            "fil_taux_pauvrete_40_49ans",
            "fil_taux_pauvrete_locataires",
            "fil_pct_revenus_salaires_chomage",
            "fil_pct_revenus_non_salaries",
        ]
        X_filo = X_filo.drop(columns=[c for c in drop_filo if c in X_filo.columns])
        X_filo = X_filo.drop(columns=X_filo.columns[X_filo.isna().all()].tolist())
        X_filo = X_filo.apply(lambda c: c.fillna(c.median()))

        X_features = X_sec.join(X_filo, how="inner")
        X_features = X_features.apply(lambda c: c.fillna(c.median()))
        X_features = X_features.loc[:, X_features.nunique() > 1]

        latest_election = (
            dim_election.sort_values(["annee_election", "tour"], ascending=[False, True])
            .iloc[0]["id_election"]
        )
        particip = fact_particip[fact_particip["id_election"] == latest_election].copy()
        particip["pct_abs_ins"] = pd.to_numeric(particip["pct_abs_ins"], errors="coerce")

        reg_df = (
            X_features.reset_index()
            .merge(particip[["id_commune", "pct_abs_ins"]], on="id_commune", how="inner")
            .merge(commune_ref, on="id_commune", how="left")
            .dropna(subset=["pct_abs_ins"])
        )

        feature_cols = X_features.columns.tolist()
        X_reg = reg_df[feature_cols].values
        y_reg = reg_df["pct_abs_ins"].values
        row_ids = reg_df.index.to_numpy()

        X_train, X_test, y_train, y_test, _, row_ids_test = train_test_split(
            X_reg, y_reg, row_ids, test_size=0.2, random_state=42
        )

        scaler = StandardScaler()
        Xtr_reg = scaler.fit_transform(X_train)
        Xte_reg = scaler.transform(X_test)

        gbr = GradientBoostingRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=3,
            subsample=0.8,
            random_state=42,
        )
        gbr.fit(Xtr_reg, y_train)
        y_pred = gbr.predict(Xte_reg)

        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        cv_r2 = cross_val_score(
            Pipeline(
                [
                    ("sc", StandardScaler()),
                    (
                        "gbr",
                        GradientBoostingRegressor(
                            n_estimators=300,
                            learning_rate=0.05,
                            max_depth=3,
                            subsample=0.8,
                            random_state=42,
                        ),
                    ),
                ]
            ),
            X_reg,
            y_reg,
            cv=5,
            scoring="r2",
        )

        test_points = reg_df.loc[row_ids_test, ["id_commune", "libelle_commune"]].copy()
        test_points["actual_abstention"] = y_test
        test_points["predicted_abstention"] = y_pred
        test_points["abs_error"] = (test_points["predicted_abstention"] - test_points["actual_abstention"]).abs()
        test_points = test_points.sort_values("actual_abstention").reset_index(drop=True)

        payload = {
            "metadata": {
                "description": "Held-out abstention regression evaluation rebuilt from the current notebook logic",
                "latest_election": latest_election,
                "latest_election_label": latest_election.replace("_", " "),
                "communes_analyzed": int(len(reg_df)),
                "test_support": int(len(test_points)),
                "latest_security_year": latest_sec_year,
                "latest_filosofi_year": latest_filo_year,
            },
            "metrics": {
                "mae": float(mae),
                "r2": float(r2),
                "cv_r2": float(cv_r2.mean()),
                "cv_r2_std": float(cv_r2.std()),
            },
            "points": test_points.round(
                {"actual_abstention": 2, "predicted_abstention": 2, "abs_error": 2}
            ).to_dict(orient="records"),
        }
        save_json(fallback_path, payload)
        return payload
    except Exception as exc:
        print(f"  ! Abstention evaluation rebuild failed: {exc}")
        return load_json(fallback_path)

# ─────────────────────────────────────────────
# 1. Dimension tables (small – ship as-is)
# ─────────────────────────────────────────────
print("Exporting dimension tables…")

dim_commune_raw = read_gold("dim_commune.csv")
dim_election  = read_gold("dim_election.csv")
dim_nuance    = read_gold("dim_nuance.csv")
dim_candidat  = read_gold("dim_candidat.csv")
dim_indicator = read_gold("dim_indicateur_securite.csv", "security")
part_raw      = read_gold("fact_participation.csv")

dim_commune_all = canonicalize_communes(dim_commune_raw)
commune_ref = dim_commune_all[["id_commune", "libelle_commune"]].copy()

part_raw["id_commune"] = part_raw["id_commune"].astype(str)
part_raw["id_election"] = part_raw["id_election"].astype(str)
coverage_per_election = (
    part_raw.groupby("id_election")["id_commune"].nunique().sort_index()
)
shared_election_ids = set.intersection(
    *[set(ids.astype(str)) for _, ids in part_raw.groupby("id_election")["id_commune"]]
)
dim_commune = (
    dim_commune_all[dim_commune_all["id_commune"].isin(shared_election_ids)]
    .sort_values("libelle_commune")
    .reset_index(drop=True)
)

save("dim_commune",   dim_commune.to_dict(orient="records"))
save("dim_election",  dim_election.to_dict(orient="records"))
save("dim_nuance",    dim_nuance.to_dict(orient="records"))
save("dim_indicateur_securite", dim_indicator.to_dict(orient="records"))

# ─────────────────────────────────────────────
# 2. Participation: abstention per commune × election
# ─────────────────────────────────────────────
print("Exporting participation…")

part = part_raw[part_raw["id_commune"].isin(shared_election_ids)].copy()
part = part.merge(commune_ref, on="id_commune", how="left")
part = part.merge(dim_election, on="id_election", how="left")

# Summary: average abstention per election
avg_abs = (
    part.groupby(["id_election","annee_election","tour"])
        ["pct_abs_ins"].mean().round(2).reset_index()
)
avg_abs["label"] = avg_abs["annee_election"].astype(str) + " T" + avg_abs["tour"].astype(str)
save("avg_abstention_per_election", avg_abs.to_dict(orient="records"))

# Per-commune per-election (for trend chart) – keep only T2 for simplicity + T1
part_slim = part[["id_commune","libelle_commune","id_election","annee_election","tour",
                   "pct_abs_ins","pct_exprimes_vot","inscrits","votants","exprimes"]].copy()
part_slim = part_slim.round(2)
save("participation_detail", part_slim.to_dict(orient="records"))

# Commune abstention 2022 T2 for ranking chart
abs_2022t2 = part[part["id_election"]=="2022_T2"][["id_commune","libelle_commune","pct_abs_ins","inscrits"]].copy()
abs_2022t2 = abs_2022t2.sort_values("pct_abs_ins", ascending=False).round(2)
save("abstention_2022_t2_ranked", abs_2022t2.to_dict(orient="records"))

# ─────────────────────────────────────────────
# 3. Election results: dominant nuance per commune × election
# ─────────────────────────────────────────────
print("Exporting election results…")

res = read_gold("fact_resultats_candidat.csv")
res["id_commune"] = res["id_commune"].astype(str)
res = res[res["id_commune"].isin(shared_election_ids)].copy()
res = res.merge(commune_ref, on="id_commune", how="left")
res = res.merge(dim_election, on="id_election", how="left")
res = res.merge(dim_nuance, on="id_nuance", how="left")

# Nuance share per election (aggregate all communes)
nuance_share = (
    res.groupby(["id_election","annee_election","tour","id_nuance","libelle_nuance"])
       ["voix"].sum().reset_index()
)
total_per_election = nuance_share.groupby("id_election")["voix"].sum().rename("total_voix")
nuance_share = nuance_share.merge(total_per_election, on="id_election")
nuance_share["pct"] = (nuance_share["voix"] / nuance_share["total_voix"] * 100).round(2)
nuance_share["label"] = nuance_share["annee_election"].astype(str) + " T" + nuance_share["tour"].astype(str)
nuance_share = nuance_share.sort_values(["id_election","pct"], ascending=[True,False])
save("nuance_share_per_election", nuance_share.to_dict(orient="records"))

# Dominant nuance per commune per election (winner)
dom = res.loc[res.groupby(["id_commune","id_election"])["voix"].idxmax()].copy()
dom = dom[["id_commune","libelle_commune","id_election","annee_election","tour","id_nuance","libelle_nuance","pct_voix_exprimes"]]
dom = dom.round(2)
save("dominant_nuance_per_commune", dom.to_dict(orient="records"))

# T2 2022: detailed scores (Macron vs Le Pen simplified view)
t2_2022 = res[res["id_election"]=="2022_T2"].copy()
t2_2022 = t2_2022.groupby(["id_commune","libelle_commune","id_nuance","libelle_nuance"])["voix"].sum().reset_index()
total_t2 = t2_2022.groupby("id_commune")["voix"].sum().rename("total")
t2_2022 = t2_2022.merge(total_t2, on="id_commune")
t2_2022["pct"] = (t2_2022["voix"] / t2_2022["total"] * 100).round(2)
save("results_2022_t2_per_commune", t2_2022.to_dict(orient="records"))

# ─────────────────────────────────────────────
# 4. ML outputs
# ─────────────────────────────────────────────
print("Exporting ML outputs…")

profiles = read_ml("commune_profiles.csv")
profiles["id_commune"] = profiles["id_commune"].astype(str)
profiles = profiles.drop_duplicates(subset=["id_commune"], keep="last")
profiles = with_commune_names(profiles, commune_ref)
profiles = profiles[profiles["vulnerability_score"].notna()].copy()
profiles = profiles.sort_values("vulnerability_score", ascending=False, na_position="last")
profiles = profiles.round({"vulnerability_score":3, "pct_abs_ins":2, "predicted_abstention":2})
save("commune_profiles", profiles.to_dict(orient="records"))

vuln = read_ml("vulnerability_scores.csv")
vuln["id_commune"] = vuln["id_commune"].astype(str)
vuln = with_commune_names(vuln, commune_ref)
vuln = vuln.sort_values("rank")
vuln["vulnerability_score"] = vuln["vulnerability_score"].round(3)
save("vulnerability_scores", vuln.to_dict(orient="records"))
cross_domain_ids = set(vuln["id_commune"].astype(str))

proj = read_ml("abstention_projections_3yr.csv", subdir="timeseries")
proj["id_commune"] = proj["id_commune"].astype(str)
proj = with_commune_names(proj, commune_ref)
proj = proj[proj["id_commune"].isin(shared_election_ids)].copy()
numeric_cols = ["slope_per_year","abstention_last_year","linear_proj_2023","linear_proj_2024",
                "linear_proj_2025","ml_proj_2023","ml_proj_2024","ml_proj_2025",
                "consensus_proj_2023","consensus_proj_2024","consensus_proj_2025","abstention_2022_observed"]
for c in numeric_cols:
    proj[c] = proj[c].round(2)
proj["risk_flag"] = proj["consensus_proj_2025"].ge(40).fillna(False)
save("abstention_projections", proj.to_dict(orient="records"))

# ─────────────────────────────────────────────
# 5. Security: taux moyen par indicateur (last available year)
# ─────────────────────────────────────────────
print("Exporting security…")

sec = read_gold("fact_securite.csv", "security")
sec["id_commune"] = sec["id_commune"].astype(str)
sec = sec[sec["est_diffuse"]=="diff"].copy()   # only diffused rows
sec = sec[sec["id_commune"].isin(cross_domain_ids)].copy()
sec["nombre"] = pd.to_numeric(sec["nombre"], errors="coerce")
sec["taux_pour_mille"] = pd.to_numeric(sec["taux_pour_mille"], errors="coerce")

# Average taux per indicator per year (all communes)
sec_agg = (
    sec.groupby(["annee","id_indicateur_securite"])
       ["taux_pour_mille"].mean().round(3).reset_index()
)
sec_agg = sec_agg.merge(dim_indicator, on="id_indicateur_securite", how="left")
sec_agg = sec_agg.sort_values(["id_indicateur_securite","annee"])
save("securite_taux_par_annee", sec_agg.to_dict(orient="records"))

# Per-commune security latest year for ranking
latest_year = int(sec["annee"].max())
sec_latest = sec[sec["annee"]==latest_year].copy()
sec_latest = sec_latest.merge(commune_ref, on="id_commune", how="left")
sec_latest = sec_latest.merge(dim_indicator, on="id_indicateur_securite", how="left")
sec_commune_agg = (
    sec_latest.groupby(["id_commune","libelle_commune","id_indicateur_securite","indicateur"])
              ["taux_pour_mille"].mean().round(3).reset_index()
)
save("securite_commune_latest", sec_commune_agg.to_dict(orient="records"))

dashboard_meta = {
    "territory_label": "Rhône (69)",
    "election_shared_communes": len(shared_election_ids),
    "cross_domain_communes": len(cross_domain_ids),
    "latest_security_year": latest_year,
    "election_coverage_by_scrutin": coverage_per_election.to_dict(),
}
save("dashboard_meta", dashboard_meta)

# ─────────────────────────────────────────────
# 6. Model performance metrics
# ─────────────────────────────────────────────
print("Exporting model metrics…")

# Notebook-derived classifier metrics (fallback gracefully to comparison.json)
comparison = extract_current_notebook_metrics()
if not comparison:
    comparison_path = ROOT / "src" / "ml" / "outputs" / "comparison.json"
    if comparison_path.exists():
        with open(comparison_path, encoding="utf-8") as f:
            comparison = json.load(f)
    else:
        comparison = {}
save("model_comparison", comparison)

abstention_evaluation = build_abstention_evaluation(dim_election, commune_ref)
save("abstention_evaluation", abstention_evaluation)

feature_importance_path = ROOT / "src" / "ml" / "outputs" / "feature_importance.json"
if feature_importance_path.exists():
    with open(feature_importance_path, encoding="utf-8") as f:
        feature_importance = json.load(f)
else:
    feature_importance = {}
save("feature_importance", feature_importance)

print("\nAll exports complete.")
