"""
Quality check engine for Electio Analytics.

Covers four domains:
  election  — gold/election/*.csv
  security  — gold/security/*.csv
  filosofi  — gold/filosofi/*.csv
  cross     — consistency across domains and layers

Each rule returns a CheckResult dataclass.

Severity levels:
  CRITICAL — data is wrong in a way that would break analytics or ML
  WARNING  — data anomaly worth investigating; does not block usage
  INFO     — informational metric, no threshold violation

Status values:
  PASS — check condition satisfied
  FAIL — check condition violated
  WARN — check condition partially violated (within warning threshold)
  SKIP — check could not run (missing file / table)
"""

from __future__ import annotations

import math
import pathlib
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
#  Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    name: str
    domain: str        # election | security | filosofi | cross
    category: str      # schema | completeness | uniqueness | integrity | business | coverage | statistical
    severity: str      # CRITICAL | WARNING | INFO
    status: str        # PASS | FAIL | WARN | SKIP
    metric: dict       # e.g. {"violations": 0, "total": 1680, "rate": 0.0}
    details: str       # human-readable sentence
    sample: list = field(default_factory=list)  # up to 5 failing row examples


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _read(path: pathlib.Path, sep: str = ";") -> pd.DataFrame | None:
    """Read CSV with automatic encoding fallback. Returns None if file missing."""
    if not path.exists():
        return None
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, sep=sep, low_memory=False, encoding=enc)
        except UnicodeDecodeError:
            continue
    return None


def _sample(df: pd.DataFrame, mask: pd.Series, n: int = 5) -> list:
    """Return up to n rows matching mask as plain dicts."""
    rows = df[mask].head(n)
    return rows.to_dict(orient="records")


def _pct(num: int, den: int) -> float:
    return round(num / den * 100, 2) if den else 0.0


def _pass(name, domain, category, severity, metric, details) -> CheckResult:
    return CheckResult(name, domain, category, severity, "PASS", metric, details)


def _fail(name, domain, category, severity, metric, details, sample=None) -> CheckResult:
    return CheckResult(name, domain, category, severity, "FAIL", metric, details, sample or [])


def _warn(name, domain, category, severity, metric, details, sample=None) -> CheckResult:
    return CheckResult(name, domain, category, severity, "WARN", metric, details, sample or [])


def _skip(name, domain, category, severity, details) -> CheckResult:
    return CheckResult(name, domain, category, severity, "SKIP", {}, details)


# ─────────────────────────────────────────────────────────────────────────────
#  QualityEngine
# ─────────────────────────────────────────────────────────────────────────────

class QualityEngine:
    """
    Runs all quality checks against the gold layer CSVs.

    Usage:
        engine = QualityEngine(project_root)
        results = engine.run_all()          # all domains
        results = engine.run_domain("election")
    """

    # Expected columns per gold table
    SCHEMA = {
        "election/dim_commune": [
            "id_commune", "code_departement", "libelle_departement",
            "code_commune", "libelle_commune",
        ],
        "election/dim_election": [
            "id_election", "annee_election", "tour",
        ],
        "election/dim_candidat": [
            "id_candidat", "nom", "prenom", "sexe",
        ],
        "election/dim_nuance": [
            "id_nuance", "libelle_nuance",
        ],
        "election/fact_participation": [
            "id_commune", "id_election", "inscrits", "abstentions", "votants",
            "blancs", "nuls", "blancs_et_nuls", "exprimes",
            "pct_abs_ins", "pct_vot_ins", "pct_blancs_ins", "pct_blancs_vot",
            "pct_nuls_ins", "pct_nuls_vot", "pct_blancs_et_nuls_ins",
            "pct_blancs_et_nuls_vot", "pct_exprimes_ins", "pct_exprimes_vot",
            "date_integration_gold",
        ],
        "election/fact_resultats_candidat": [
            "id_commune", "id_election", "id_candidat", "id_nuance",
            "voix", "pct_voix_ins", "pct_voix_exprimes", "date_integration_gold",
        ],
        "security/dim_indicateur_securite": [
            "id_indicateur_securite", "indicateur", "unite_de_compte",
        ],
        "security/dim_departement": [
            "id_departement", "code_region",
        ],
        "security/fact_securite": [
            "id_commune", "annee", "id_indicateur_securite", "nombre",
            "taux_pour_mille", "est_diffuse",
        ],
        "security/fact_securite_dep": [
            "id_departement", "annee", "id_indicateur_securite",
            "nombre", "taux_pour_mille",
        ],
        "security/fact_demographie": [
            "id_commune", "annee", "insee_pop", "insee_log",
        ],
        "security/fact_demographie_dep": [
            "id_departement", "annee", "insee_pop", "insee_log",
        ],
        "filosofi/dim_time": [
            "id_year", "annee",
        ],
        "filosofi/dim_commune": [
            "id_commune", "code_departement", "code_commune",
            "code_insee_commune", "libelle_commune",
        ],
        "filosofi/fact_menages": [
            "id_commune", "id_year", "nb_menages_fiscaux",
            "nb_personnes_menages_fiscaux", "mediane_niveau_vie_euros",
            "pct_menages_imposables",
        ],
        "filosofi/fact_revenus": [
            "id_commune", "id_year", "pct_revenus_activite",
            "pct_revenus_salaires_chomage", "pct_revenus_activite_non_salariee",
            "pct_pensions_retraites", "pct_revenus_patrimoine",
            "pct_prestations_sociales",
        ],
        "filosofi/fact_pauvrete": [
            "id_commune", "id_year", "taux_pauvrete_ensemble",
            "taux_pauvrete_moins_30ans",
        ],
        "filosofi/fact_deciles": [
            "id_commune", "id_year", "decile_1_revenu",
            "decile_9_revenu", "ratio_deciles",
        ],
    }

    # Expected row count ranges (min, max) for sanity checks
    ROW_COUNTS = {
        "election/fact_participation":       (1500, 2000),
        "election/fact_resultats_candidat":  (8000, 15000),
        "election/dim_commune":              (250, 350),
        "election/dim_election":             (6,   6),
        "election/dim_nuance":               (50,  150),
        "election/dim_candidat":             (15,  40),
        "security/fact_securite":            (30000, 45000),
        "security/fact_demographie":         (2000, 3000),
        "filosofi/fact_menages":             (1500, 3000),
    }

    def __init__(self, root: pathlib.Path):
        self.root = pathlib.Path(root)
        self.gold = self.root / "data" / "gold"
        self._cache: dict[str, pd.DataFrame | None] = {}

    def _load(self, key: str) -> pd.DataFrame | None:
        """Load and cache a gold table by its schema key (e.g. 'election/dim_commune')."""
        if key not in self._cache:
            path = self.gold / f"{key}.csv"
            self._cache[key] = _read(path)
        return self._cache[key]

    # ──────────────────────────────────────────────────────────────────────
    #  Public API
    # ──────────────────────────────────────────────────────────────────────

    def run_all(self) -> list[CheckResult]:
        results = []
        for domain in ("election", "security", "filosofi", "cross"):
            results += self.run_domain(domain)
        return results

    def run_domain(self, domain: str) -> list[CheckResult]:
        dispatch = {
            "election": self._check_election,
            "security": self._check_security,
            "filosofi": self._check_filosofi,
            "cross":    self._check_cross,
        }
        fn = dispatch.get(domain)
        if fn is None:
            raise ValueError(f"Unknown domain: {domain!r}. Choose from: {list(dispatch)}")
        return fn()

    # ──────────────────────────────────────────────────────────────────────
    #  Generic reusable rule builders
    # ──────────────────────────────────────────────────────────────────────

    def _schema_check(self, key: str, domain: str) -> list[CheckResult]:
        """Verify all expected columns are present."""
        results = []
        df = self._load(key)
        table = key.split("/")[1]
        name = f"{domain}.{table}.schema"
        expected = self.SCHEMA.get(key, [])

        if df is None:
            return [_skip(name, domain, "schema", "CRITICAL",
                          f"File not found: data/gold/{key}.csv")]

        missing = [c for c in expected if c not in df.columns]
        extra   = [c for c in df.columns if c not in expected]
        if missing:
            results.append(_fail(
                name, domain, "schema", "CRITICAL",
                {"missing_columns": missing, "extra_columns": extra, "found": list(df.columns)},
                f"{table}: {len(missing)} required column(s) missing: {missing}",
            ))
        else:
            results.append(_pass(
                name, domain, "schema", "CRITICAL",
                {"columns_ok": len(expected), "extra_columns": extra},
                f"{table}: all {len(expected)} required columns present."
                + (f" {len(extra)} extra column(s) found (ignored)." if extra else ""),
            ))
        return results

    def _row_count_check(self, key: str, domain: str) -> list[CheckResult]:
        """Check row count is within expected range."""
        df = self._load(key)
        table = key.split("/")[1]
        name  = f"{domain}.{table}.row_count"
        lo, hi = self.ROW_COUNTS.get(key, (1, 10_000_000))
        if df is None:
            return [_skip(name, domain, "schema", "CRITICAL", f"File missing: {key}.csv")]
        n = len(df)
        metric = {"rows": n, "expected_min": lo, "expected_max": hi}
        if n < lo:
            return [_fail(name, domain, "schema", "CRITICAL", metric,
                          f"{table}: only {n} rows — expected at least {lo}.")]
        if n > hi:
            return [_warn(name, domain, "schema", "WARNING", metric,
                          f"{table}: {n} rows — exceeds expected max {hi}. Verify scope.")]
        return [_pass(name, domain, "schema", "INFO", metric,
                      f"{table}: {n} rows — within expected range [{lo}, {hi}].")]

    def _completeness_check(
        self,
        key: str,
        domain: str,
        mandatory_cols: list[str],
        nullable_cols: list[str] | None = None,
        warn_threshold: float = 0.05,
        fail_threshold: float = 0.50,
    ) -> list[CheckResult]:
        """
        Check null rates.
        mandatory_cols   — any null is a CRITICAL FAIL
        other columns    — null rate > fail_threshold is CRITICAL, > warn_threshold is WARNING
        """
        df = self._load(key)
        table = key.split("/")[1]
        results = []

        if df is None:
            return [_skip(f"{domain}.{table}.completeness", domain, "completeness", "CRITICAL",
                          f"File missing: {key}.csv")]

        nullable_cols = nullable_cols or []
        n = len(df)

        # Mandatory: zero-null expected
        for col in mandatory_cols:
            if col not in df.columns:
                continue
            nulls = int(df[col].isnull().sum())
            name = f"{domain}.{table}.completeness.{col}"
            metric = {"nulls": nulls, "total": n, "null_rate": _pct(nulls, n)}
            if nulls > 0:
                results.append(_fail(
                    name, domain, "completeness", "CRITICAL", metric,
                    f"{table}.{col}: {nulls} null(s) in a mandatory column ({_pct(nulls, n):.1f}%).",
                    _sample(df, df[col].isnull()),
                ))
            else:
                results.append(_pass(name, domain, "completeness", "CRITICAL", metric,
                                     f"{table}.{col}: 0 nulls — complete."))

        # Non-mandatory: threshold-based
        optional = [c for c in df.columns if c not in mandatory_cols and c not in nullable_cols]
        for col in optional:
            nulls = int(df[col].isnull().sum())
            rate  = nulls / n if n else 0
            name  = f"{domain}.{table}.completeness.{col}"
            metric = {"nulls": nulls, "total": n, "null_rate": round(rate * 100, 2)}
            if rate > fail_threshold:
                results.append(_fail(
                    name, domain, "completeness", "WARNING", metric,
                    f"{table}.{col}: {_pct(nulls, n):.1f}% null — exceeds {fail_threshold*100:.0f}% threshold.",
                ))
            elif rate > warn_threshold:
                results.append(_warn(
                    name, domain, "completeness", "WARNING", metric,
                    f"{table}.{col}: {_pct(nulls, n):.1f}% null — above {warn_threshold*100:.0f}% warning threshold.",
                ))
            else:
                results.append(_pass(name, domain, "completeness", "INFO", metric,
                                     f"{table}.{col}: {_pct(nulls, n):.1f}% null — acceptable."))

        return results

    def _pk_uniqueness_check(self, key: str, domain: str, pk_cols: list[str]) -> CheckResult:
        """Verify primary key uniqueness."""
        df = self._load(key)
        table = key.split("/")[1]
        name  = f"{domain}.{table}.pk_uniqueness"
        if df is None:
            return _skip(name, domain, "uniqueness", "CRITICAL", f"File missing: {key}.csv")

        present = [c for c in pk_cols if c in df.columns]
        if len(present) != len(pk_cols):
            return _skip(name, domain, "uniqueness", "CRITICAL",
                         f"PK column(s) missing: {set(pk_cols) - set(present)}")

        n        = len(df)
        n_unique = len(df[present].drop_duplicates())
        dupes    = n - n_unique
        metric   = {"duplicates": dupes, "total": n, "pk": pk_cols}

        if dupes > 0:
            dup_mask = df.duplicated(subset=present, keep=False)
            return _fail(name, domain, "uniqueness", "CRITICAL", metric,
                         f"{table}: {dupes} duplicate PK row(s) on {pk_cols}.",
                         _sample(df, dup_mask))
        return _pass(name, domain, "uniqueness", "CRITICAL", metric,
                     f"{table}: PK {pk_cols} unique across all {n} rows.")

    def _fk_check(
        self,
        fact_key: str, fk_col: str,
        dim_key: str,  pk_col: str,
        domain: str,
    ) -> CheckResult:
        """Verify every FK value in fact has a matching PK in the dim table."""
        fact = self._load(fact_key)
        dim  = self._load(dim_key)
        fact_table = fact_key.split("/")[1]
        dim_table  = dim_key.split("/")[1]
        name = f"{domain}.{fact_table}.fk.{fk_col}"

        if fact is None:
            return _skip(name, domain, "integrity", "CRITICAL", f"Fact file missing: {fact_key}.csv")
        if dim is None:
            return _skip(name, domain, "integrity", "CRITICAL", f"Dim file missing: {dim_key}.csv")
        if fk_col not in fact.columns:
            return _skip(name, domain, "integrity", "CRITICAL", f"{fact_table}.{fk_col} column missing.")
        if pk_col not in dim.columns:
            return _skip(name, domain, "integrity", "CRITICAL", f"{dim_table}.{pk_col} column missing.")

        valid  = set(dim[pk_col].dropna().astype(str))
        fk_ser = fact[fk_col].astype(str)
        orphan_mask = ~fk_ser.isin(valid)
        orphans = int(orphan_mask.sum())
        n = len(fact)
        metric = {"orphans": orphans, "total": n,
                  "orphan_rate": _pct(orphans, n),
                  "fk": f"{fact_table}.{fk_col} → {dim_table}.{pk_col}"}

        if orphans > 0:
            return _fail(name, domain, "integrity", "CRITICAL", metric,
                         f"{orphans} row(s) in {fact_table}.{fk_col} have no match in {dim_table}.{pk_col} "
                         f"({_pct(orphans, n):.2f}%).",
                         _sample(fact, orphan_mask))
        return _pass(name, domain, "integrity", "CRITICAL", metric,
                     f"{fact_table}.{fk_col} → {dim_table}.{pk_col}: 100% referential integrity ({n} rows).")

    def _range_check(
        self,
        key: str, col: str, domain: str,
        lo: float | None = None, hi: float | None = None,
        severity: str = "CRITICAL",
    ) -> CheckResult:
        """Check a numeric column stays within [lo, hi]."""
        df = self._load(key)
        table = key.split("/")[1]
        name  = f"{domain}.{table}.range.{col}"
        if df is None or col not in df.columns:
            return _skip(name, domain, "business", severity,
                         f"{'File' if df is None else 'Column'} missing: {key}/{col}")

        series = pd.to_numeric(df[col], errors="coerce")
        n = len(series.dropna())
        mask = pd.Series([False] * len(df))
        if lo is not None:
            mask |= series < lo
        if hi is not None:
            mask |= series > hi
        mask &= series.notna()
        violations = int(mask.sum())
        metric = {"violations": violations, "checked": n,
                  "range": f"[{lo}, {hi}]",
                  "actual_min": round(float(series.min()), 4) if n else None,
                  "actual_max": round(float(series.max()), 4) if n else None}

        if violations:
            return _fail(name, domain, "business", severity, metric,
                         f"{table}.{col}: {violations} value(s) outside [{lo}, {hi}].",
                         _sample(df, mask))
        return _pass(name, domain, "business", severity, metric,
                     f"{table}.{col}: all {n} non-null values within [{lo}, {hi}].")

    def _enum_check(
        self, key: str, col: str, domain: str,
        allowed: set, severity: str = "CRITICAL",
    ) -> CheckResult:
        """Check a column only contains values from an allowed set."""
        df = self._load(key)
        table = key.split("/")[1]
        name  = f"{domain}.{table}.enum.{col}"
        if df is None or col not in df.columns:
            return _skip(name, domain, "business", severity,
                         f"{'File' if df is None else 'Column'} missing.")
        unexpected = set(df[col].dropna().astype(str)) - {str(v) for v in allowed}
        n = len(df)
        mask = df[col].astype(str).isin({str(v) for v in unexpected})
        metric = {"unexpected_values": list(unexpected), "total": n}
        if unexpected:
            return _fail(name, domain, "business", severity, metric,
                         f"{table}.{col}: unexpected values {unexpected}.",
                         _sample(df, mask))
        return _pass(name, domain, "business", severity, metric,
                     f"{table}.{col}: all values within allowed set {allowed}.")

    # ──────────────────────────────────────────────────────────────────────
    #  ELECTION DOMAIN
    # ──────────────────────────────────────────────────────────────────────

    def _check_election(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        D = "election"

        # ── Schema & row counts ───────────────────────────────────────────
        for key in [
            "election/dim_commune", "election/dim_election",
            "election/dim_candidat", "election/dim_nuance",
            "election/fact_participation", "election/fact_resultats_candidat",
        ]:
            results += self._schema_check(key, D)
        for key in [
            "election/fact_participation", "election/fact_resultats_candidat",
            "election/dim_commune", "election/dim_election", "election/dim_nuance",
        ]:
            results += self._row_count_check(key, D)

        # ── Completeness ─────────────────────────────────────────────────
        results += self._completeness_check(
            "election/fact_participation", D,
            mandatory_cols=["id_commune", "id_election", "inscrits",
                            "abstentions", "votants", "exprimes"],
            nullable_cols=["date_integration_gold"],
        )
        results += self._completeness_check(
            "election/fact_resultats_candidat", D,
            mandatory_cols=["id_commune", "id_election", "id_candidat", "id_nuance", "voix"],
            nullable_cols=["date_integration_gold"],
        )
        results += self._completeness_check(
            "election/dim_commune", D,
            mandatory_cols=["id_commune", "code_departement", "code_commune", "libelle_commune"],
        )

        # ── PK uniqueness ─────────────────────────────────────────────────
        # Note: duplicate id_commune entries are expected for fused communes (pre/post-fusion
        # names both kept in the dim). Flagged as CRITICAL — downstream joins will fan-out
        # unless the consuming query filters to a single libelle_commune per id.
        results.append(self._pk_uniqueness_check("election/dim_commune", D, ["id_commune"]))
        results.append(self._pk_uniqueness_check("election/dim_election", D, ["id_election"]))
        results.append(self._pk_uniqueness_check("election/dim_candidat", D, ["id_candidat"]))
        results.append(self._pk_uniqueness_check("election/dim_nuance", D, ["id_nuance"]))
        results.append(self._pk_uniqueness_check(
            "election/fact_participation", D, ["id_commune", "id_election"]))
        results.append(self._pk_uniqueness_check(
            "election/fact_resultats_candidat", D, ["id_commune", "id_election", "id_candidat"]))

        # ── Referential integrity ─────────────────────────────────────────
        for fk_col in ["id_commune", "id_election"]:
            results.append(self._fk_check(
                "election/fact_participation", fk_col,
                f"election/dim_{fk_col.split('_')[1]}", fk_col, D))
        results.append(self._fk_check(
            "election/fact_resultats_candidat", "id_commune",
            "election/dim_commune", "id_commune", D))
        results.append(self._fk_check(
            "election/fact_resultats_candidat", "id_election",
            "election/dim_election", "id_election", D))
        results.append(self._fk_check(
            "election/fact_resultats_candidat", "id_candidat",
            "election/dim_candidat", "id_candidat", D))
        results.append(self._fk_check(
            "election/fact_resultats_candidat", "id_nuance",
            "election/dim_nuance", "id_nuance", D))

        # ── Business rules — fact_participation ───────────────────────────
        part = self._load("election/fact_participation")
        if part is not None:
            # R1: inscrits > 0
            mask = part["inscrits"] <= 0
            n = len(part)
            v = int(mask.sum())
            results.append(
                _fail("election.fact_participation.business.inscrits_positive",
                      D, "business", "CRITICAL",
                      {"violations": v, "total": n},
                      f"inscrits <= 0 in {v} row(s).", _sample(part, mask))
                if v else
                _pass("election.fact_participation.business.inscrits_positive",
                      D, "business", "CRITICAL",
                      {"violations": 0, "total": n},
                      f"inscrits > 0 for all {n} rows.")
            )

            # R2: inscrits = abstentions + votants (within 1, rounding tolerance)
            diff = (part["inscrits"] - part["abstentions"] - part["votants"]).abs()
            mask = diff > 1
            v = int(mask.sum())
            results.append(
                _fail("election.fact_participation.business.inscrits_split",
                      D, "business", "CRITICAL",
                      {"violations": v, "total": n, "rule": "inscrits = abstentions + votants"},
                      f"{v} row(s) violate: inscrits ≠ abstentions + votants (tolerance 1).",
                      _sample(part, mask))
                if v else
                _pass("election.fact_participation.business.inscrits_split",
                      D, "business", "CRITICAL",
                      {"violations": 0, "total": n},
                      f"inscrits = abstentions + votants holds for all {n} rows (tolerance ±1).")
            )

            # R3: votants = blancs_et_nuls + exprimes (within 1)
            diff = (part["votants"] - part["blancs_et_nuls"] - part["exprimes"]).abs()
            mask = diff > 1
            v = int(mask.sum())
            results.append(
                _fail("election.fact_participation.business.votants_split",
                      D, "business", "CRITICAL",
                      {"violations": v, "total": n, "rule": "votants = blancs_et_nuls + exprimes"},
                      f"{v} row(s) violate: votants ≠ blancs_et_nuls + exprimes (tolerance 1).",
                      _sample(part, mask))
                if v else
                _pass("election.fact_participation.business.votants_split",
                      D, "business", "CRITICAL",
                      {"violations": 0, "total": n},
                      f"votants = blancs_et_nuls + exprimes holds for all {n} rows.")
            )

            # R4: blancs + nuls = blancs_et_nuls (only 2017+ rows where both are non-zero)
            subset = part[(part["blancs"] > 0) & (part["nuls"] > 0)].copy()
            if len(subset):
                diff = (subset["blancs"] + subset["nuls"] - subset["blancs_et_nuls"]).abs()
                mask = diff > 1
                v = int(mask.sum())
                results.append(
                    _fail("election.fact_participation.business.blancs_nuls_sum",
                          D, "business", "WARNING",
                          {"violations": v, "total": len(subset)},
                          f"{v}/{len(subset)} row(s): blancs + nuls ≠ blancs_et_nuls (tolerance 1).",
                          _sample(subset, mask))
                    if v else
                    _pass("election.fact_participation.business.blancs_nuls_sum",
                          D, "business", "WARNING",
                          {"violations": 0, "total": len(subset)},
                          f"blancs + nuls = blancs_et_nuls for all {len(subset)} rows where both columns are populated.")
                )

            # R5: percentages in [0, 100]
            pct_cols = [c for c in part.columns if c.startswith("pct_")]
            for col in pct_cols:
                results.append(self._range_check(
                    "election/fact_participation", col, D, 0.0, 100.0, "CRITICAL"))

            # R6: pct_abs_ins + pct_vot_ins ≈ 100 (within 0.2)
            total_pct = part["pct_abs_ins"] + part["pct_vot_ins"]
            mask = (total_pct - 100).abs() > 0.2
            v = int(mask.sum())
            results.append(
                _fail("election.fact_participation.business.pct_abs_vot_sum",
                      D, "business", "WARNING",
                      {"violations": v, "total": n, "rule": "pct_abs_ins + pct_vot_ins ≈ 100"},
                      f"{v} row(s): pct_abs_ins + pct_vot_ins deviates from 100% by >0.2.",
                      _sample(part, mask))
                if v else
                _pass("election.fact_participation.business.pct_abs_vot_sum",
                      D, "business", "WARNING",
                      {"violations": 0, "total": n},
                      f"pct_abs_ins + pct_vot_ins ≈ 100% for all {n} rows (tolerance ±0.2).")
            )

            # R7: Abstention outliers — communes with pct_abs_ins > 60 are suspicious
            outliers = part[part["pct_abs_ins"] > 60]
            results.append(
                _warn("election.fact_participation.statistical.abstention_outliers",
                      D, "statistical", "WARNING",
                      {"outlier_count": len(outliers), "threshold": 60,
                       "max_observed": round(float(part["pct_abs_ins"].max()), 2)},
                      f"{len(outliers)} row(s) with abstention > 60% (statistical outlier check).",
                      outliers.head(5).to_dict(orient="records"))
                if len(outliers) else
                _pass("election.fact_participation.statistical.abstention_outliers",
                      D, "statistical", "INFO",
                      {"outlier_count": 0, "threshold": 60,
                       "max_observed": round(float(part["pct_abs_ins"].max()), 2)},
                      f"No abstention rate exceeds 60%. Max observed: {part['pct_abs_ins'].max():.1f}%.")
            )

        # ── Business rules — fact_resultats_candidat ──────────────────────
        res = self._load("election/fact_resultats_candidat")
        if res is not None:
            # voix >= 0
            results.append(self._range_check(
                "election/fact_resultats_candidat", "voix", D, 0, None, "CRITICAL"))
            # pct_voix_ins and pct_voix_exprimes in [0, 100]
            results.append(self._range_check(
                "election/fact_resultats_candidat", "pct_voix_ins", D, 0.0, 100.0, "CRITICAL"))
            results.append(self._range_check(
                "election/fact_resultats_candidat", "pct_voix_exprimes", D, 0.0, 100.0, "CRITICAL"))

            # Sum of voix per (commune, election) should ≈ exprimes in fact_participation
            if part is not None:
                voix_sum = (res.groupby(["id_commune", "id_election"])["voix"]
                              .sum().reset_index()
                              .rename(columns={"voix": "voix_sum"}))
                joined = part.merge(voix_sum, on=["id_commune", "id_election"], how="left")
                joined["voix_sum"] = joined["voix_sum"].fillna(0)
                diff = (joined["exprimes"] - joined["voix_sum"]).abs()
                mask_j = diff > joined["exprimes"] * 0.01  # 1% tolerance
                v = int(mask_j.sum())
                results.append(
                    _fail("election.fact_resultats_candidat.business.voix_sum_vs_exprimes",
                          D, "business", "WARNING",
                          {"violations": v, "total": len(joined),
                           "rule": "Σ voix ≈ exprimes (1% tolerance)"},
                          f"{v} (commune, election) pair(s): sum of voix deviates from exprimes by >1%.",
                          _sample(joined, mask_j))
                    if v else
                    _pass("election.fact_resultats_candidat.business.voix_sum_vs_exprimes",
                          D, "business", "WARNING",
                          {"violations": 0, "total": len(joined)},
                          f"Σ voix ≈ exprimes for all {len(joined)} (commune, election) pairs (1% tolerance).")
                )

        # ── Coverage ──────────────────────────────────────────────────────
        dim_c = self._load("election/dim_commune")
        dim_e = self._load("election/dim_election")
        if part is not None and dim_c is not None and dim_e is not None:
            n_communes   = int(dim_c["id_commune"].astype(str).nunique())
            n_elections  = len(dim_e)
            n_expected   = n_communes * n_elections
            n_actual     = len(part[["id_commune", "id_election"]].drop_duplicates())
            missing_pairs = n_expected - n_actual
            coverage_pct  = _pct(n_actual, n_expected)

            msg = (f"fact_participation coverage: {n_actual}/{n_expected} "
                   f"(commune × election) pairs = {coverage_pct:.1f}%. "
                   f"{missing_pairs} missing pairs (expected due to commune fusions 2017→2022).")
            results.append(
                _warn("election.fact_participation.coverage.commune_election_pairs",
                      D, "coverage", "WARNING",
                      {"pairs_found": n_actual, "pairs_expected": n_expected,
                       "coverage_pct": coverage_pct, "missing": missing_pairs},
                      msg)
                if missing_pairs > 0 else
                _pass("election.fact_participation.coverage.commune_election_pairs",
                      D, "coverage", "INFO",
                      {"pairs_found": n_actual, "pairs_expected": n_expected, "coverage_pct": 100.0},
                      f"All {n_expected} (commune × election) pairs present in fact_participation.")
            )

        # ── dim_election content ──────────────────────────────────────────
        dim_e = self._load("election/dim_election")
        if dim_e is not None:
            expected_elections = {"2012_T1", "2012_T2", "2017_T1", "2017_T2", "2022_T1", "2022_T2"}
            found = set(dim_e["id_election"].astype(str))
            missing_elec = expected_elections - found
            results.append(
                _fail("election.dim_election.business.expected_elections",
                      D, "business", "CRITICAL",
                      {"missing": list(missing_elec), "found": list(found)},
                      f"Missing elections in dim_election: {missing_elec}.")
                if missing_elec else
                _pass("election.dim_election.business.expected_elections",
                      D, "business", "CRITICAL",
                      {"elections": sorted(found)},
                      f"All 6 expected elections present: {sorted(found)}.")
            )

            # Tours must be 1 or 2
            results.append(self._enum_check("election/dim_election", "tour", D,
                                            {1, 2}, "CRITICAL"))

        # ── Département scope (all communes must be code 69) ──────────────
        dim_c = self._load("election/dim_commune")
        if dim_c is not None:
            unexpected_dept = dim_c[dim_c["code_departement"].astype(str) != "69"]
            v = len(unexpected_dept)
            results.append(
                _fail("election.dim_commune.business.departement_scope",
                      D, "business", "CRITICAL",
                      {"violations": v, "expected_dept": "69"},
                      f"{v} commune(s) outside Rhône (code 69).",
                      unexpected_dept.head(5).to_dict(orient="records"))
                if v else
                _pass("election.dim_commune.business.departement_scope",
                      D, "business", "CRITICAL",
                      {"communes": len(dim_c), "dept": "69"},
                      f"All {len(dim_c)} communes are in Rhône (code_departement = 69).")
            )

        return results

    # ──────────────────────────────────────────────────────────────────────
    #  SECURITY DOMAIN
    # ──────────────────────────────────────────────────────────────────────

    def _check_security(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        D = "security"

        # ── Schema & row counts ───────────────────────────────────────────
        for key in [
            "security/dim_indicateur_securite", "security/dim_departement",
            "security/fact_securite", "security/fact_securite_dep",
            "security/fact_demographie", "security/fact_demographie_dep",
        ]:
            results += self._schema_check(key, D)
        for key in ["security/fact_securite", "security/fact_demographie"]:
            results += self._row_count_check(key, D)

        # ── Completeness ─────────────────────────────────────────────────
        results += self._completeness_check(
            "security/fact_securite", D,
            mandatory_cols=["id_commune", "annee", "id_indicateur_securite", "est_diffuse"],
            nullable_cols=["nombre", "taux_pour_mille",
                           "complement_info_nombre", "complement_info_taux"],
        )
        results += self._completeness_check(
            "security/fact_demographie", D,
            mandatory_cols=["id_commune", "annee", "insee_pop", "insee_log"],
        )

        # ── PK uniqueness ─────────────────────────────────────────────────
        results.append(self._pk_uniqueness_check(
            "security/dim_indicateur_securite", D, ["id_indicateur_securite"]))
        results.append(self._pk_uniqueness_check(
            "security/fact_securite", D,
            ["id_commune", "annee", "id_indicateur_securite"]))
        results.append(self._pk_uniqueness_check(
            "security/fact_demographie", D, ["id_commune", "annee"]))
        results.append(self._pk_uniqueness_check(
            "security/fact_securite_dep", D,
            ["id_departement", "annee", "id_indicateur_securite"]))

        # ── Referential integrity ─────────────────────────────────────────
        results.append(self._fk_check(
            "security/fact_securite", "id_indicateur_securite",
            "security/dim_indicateur_securite", "id_indicateur_securite", D))
        results.append(self._fk_check(
            "security/fact_securite_dep", "id_indicateur_securite",
            "security/dim_indicateur_securite", "id_indicateur_securite", D))
        results.append(self._fk_check(
            "security/fact_securite_dep", "id_departement",
            "security/dim_departement", "id_departement", D))

        # ── Business rules ────────────────────────────────────────────────
        sec = self._load("security/fact_securite")
        if sec is not None:
            n = len(sec)

            # est_diffuse is exactly 'diff' or 'ndiff'
            results.append(self._enum_check(
                "security/fact_securite", "est_diffuse", D,
                {"diff", "ndiff"}, "CRITICAL"))

            # annee in expected range [2016, 2024]
            results.append(self._range_check(
                "security/fact_securite", "annee", D, 2016, 2024, "CRITICAL"))

            # taux_pour_mille >= 0 for 'diff' rows only
            diff_rows = sec[sec["est_diffuse"] == "diff"].copy()
            if len(diff_rows):
                diff_rows["taux_pour_mille"] = pd.to_numeric(
                    diff_rows["taux_pour_mille"], errors="coerce")
                neg_mask = diff_rows["taux_pour_mille"] < 0
                v = int(neg_mask.sum())
                results.append(
                    _fail("security.fact_securite.business.taux_non_negative",
                          D, "business", "CRITICAL",
                          {"violations": v, "checked": len(diff_rows)},
                          f"{v} 'diff' row(s) with negative taux_pour_mille.",
                          _sample(diff_rows, neg_mask))
                    if v else
                    _pass("security.fact_securite.business.taux_non_negative",
                          D, "business", "CRITICAL",
                          {"violations": 0, "checked": len(diff_rows)},
                          f"taux_pour_mille >= 0 for all {len(diff_rows)} diffused rows.")
                )

            # Expected 18 indicators
            dim_ind = self._load("security/dim_indicateur_securite")
            if dim_ind is not None:
                n_ind = len(dim_ind)
                results.append(
                    _fail("security.dim_indicateur_securite.business.indicator_count",
                          D, "business", "WARNING",
                          {"found": n_ind, "expected": 18},
                          f"Expected 18 security indicators, found {n_ind}.")
                    if n_ind != 18 else
                    _pass("security.dim_indicateur_securite.business.indicator_count",
                          D, "business", "INFO",
                          {"count": n_ind},
                          f"All 18 security indicators present in dim_indicateur_securite.")
                )

            # Secret statistique (ndiff) rate
            ndiff_count = int((sec["est_diffuse"] == "ndiff").sum())
            ndiff_rate  = _pct(ndiff_count, n)
            results.append(_pass(
                "security.fact_securite.coverage.secret_statistique_rate",
                D, "coverage", "INFO",
                {"ndiff_rows": ndiff_count, "total": n,
                 "ndiff_rate_pct": ndiff_rate,
                 "note": "INSEE secret statistique: values suppressed for small zones (<11 households or <31 persons)"},
                f"INSEE secret statistique: {ndiff_count}/{n} rows non-diffused ({ndiff_rate:.1f}%). "
                "This is expected behaviour for small communes."
            ))

            # Coverage: % of communes with security data per year
            demo = self._load("security/fact_demographie")
            if demo is not None:
                communes_in_demo = demo["id_commune"].nunique()
                communes_in_sec  = sec["id_commune"].nunique()
                coverage = _pct(communes_in_sec, communes_in_demo)
                results.append(_pass(
                    "security.fact_securite.coverage.commune_coverage",
                    D, "coverage", "INFO",
                    {"communes_with_security": communes_in_sec,
                     "communes_in_demo": communes_in_demo,
                     "coverage_pct": coverage},
                    f"Security data available for {communes_in_sec}/{communes_in_demo} communes "
                    f"({coverage:.1f}% of demographic scope)."
                ))

            # Year coverage completeness — expected 2016 through latest available year
            years_present = sorted(sec["annee"].unique())
            max_expected  = int(years_present[-1]) if years_present else 2024
            missing_years = [y for y in range(2016, max_expected + 1) if y not in years_present]
            results.append(
                _warn("security.fact_securite.coverage.year_coverage",
                      D, "coverage", "WARNING",
                      {"years_present": years_present, "missing_years": missing_years},
                      f"Missing security years: {missing_years}.")
                if missing_years else
                _pass("security.fact_securite.coverage.year_coverage",
                      D, "coverage", "INFO",
                      {"years_present": years_present},
                      f"Security data present for all years 2016–{max_expected}: {years_present}.")
            )

        # ── Demographie business rules ────────────────────────────────────
        demo = self._load("security/fact_demographie")
        if demo is not None:
            results.append(self._range_check(
                "security/fact_demographie", "insee_pop", D, 1, None, "WARNING"))
            results.append(self._range_check(
                "security/fact_demographie", "insee_log", D, 0, None, "WARNING"))
            results.append(self._range_check(
                "security/fact_demographie", "annee", D, 2016, 2024, "CRITICAL"))

        return results

    # ──────────────────────────────────────────────────────────────────────
    #  FILOSOFI DOMAIN
    # ──────────────────────────────────────────────────────────────────────

    def _check_filosofi(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        D = "filosofi"

        # ── Schema ────────────────────────────────────────────────────────
        for key in [
            "filosofi/dim_time", "filosofi/dim_commune",
            "filosofi/fact_menages", "filosofi/fact_revenus",
            "filosofi/fact_pauvrete", "filosofi/fact_deciles",
        ]:
            results += self._schema_check(key, D)

        results += self._row_count_check("filosofi/fact_menages", D)

        # ── Completeness (FILOSOFI has expected sparsity for small communes) ──
        results += self._completeness_check(
            "filosofi/fact_menages", D,
            mandatory_cols=["id_commune", "id_year"],
            nullable_cols=["nb_menages_fiscaux", "nb_personnes_menages_fiscaux",
                           "mediane_niveau_vie_euros", "pct_menages_imposables"],
            warn_threshold=0.20,
            fail_threshold=0.70,
        )
        # Poverty and deciles are very sparse for small communes — document not fail
        results += self._completeness_check(
            "filosofi/fact_pauvrete", D,
            mandatory_cols=["id_commune", "id_year"],
            nullable_cols=["taux_pauvrete_ensemble", "taux_pauvrete_moins_30ans",
                           "taux_pauvrete_30_39ans", "taux_pauvrete_40_49ans",
                           "taux_pauvrete_50_59ans", "taux_pauvrete_60_74ans",
                           "taux_pauvrete_75ans_plus", "taux_pauvrete_proprietaires",
                           "taux_pauvrete_locataires"],
            warn_threshold=0.60,
            fail_threshold=0.95,
        )
        results += self._completeness_check(
            "filosofi/fact_deciles", D,
            mandatory_cols=["id_commune", "id_year"],
            nullable_cols=["decile_1_revenu", "decile_9_revenu", "ratio_deciles"],
            warn_threshold=0.50,
            fail_threshold=0.95,
        )

        # ── PK uniqueness ─────────────────────────────────────────────────
        results.append(self._pk_uniqueness_check("filosofi/dim_time", D, ["id_year"]))
        results.append(self._pk_uniqueness_check(
            "filosofi/fact_menages", D, ["id_commune", "id_year"]))
        results.append(self._pk_uniqueness_check(
            "filosofi/fact_revenus", D, ["id_commune", "id_year"]))
        results.append(self._pk_uniqueness_check(
            "filosofi/fact_pauvrete", D, ["id_commune", "id_year"]))
        results.append(self._pk_uniqueness_check(
            "filosofi/fact_deciles", D, ["id_commune", "id_year"]))

        # ── Referential integrity ─────────────────────────────────────────
        for fact_key in ["filosofi/fact_menages", "filosofi/fact_revenus",
                          "filosofi/fact_pauvrete", "filosofi/fact_deciles"]:
            results.append(self._fk_check(
                fact_key, "id_year", "filosofi/dim_time", "id_year", D))

        # ── Business rules ────────────────────────────────────────────────
        dim_time = self._load("filosofi/dim_time")
        if dim_time is not None:
            expected_years = set(range(2014, 2022))
            found_years    = set(dim_time["annee"].astype(int))
            missing_years  = expected_years - found_years
            results.append(
                _fail("filosofi.dim_time.business.year_coverage",
                      D, "business", "CRITICAL",
                      {"missing": sorted(missing_years), "found": sorted(found_years)},
                      f"Missing years in dim_time: {sorted(missing_years)}.")
                if missing_years else
                _pass("filosofi.dim_time.business.year_coverage",
                      D, "business", "CRITICAL",
                      {"years": sorted(found_years)},
                      f"All years 2014–2021 present in dim_time: {sorted(found_years)}.")
            )

        menages = self._load("filosofi/fact_menages")
        if menages is not None:
            # mediane_niveau_vie_euros must be > 0 when not null
            col = "mediane_niveau_vie_euros"
            if col in menages.columns:
                series = pd.to_numeric(menages[col], errors="coerce")
                neg_mask = series <= 0
                neg_mask &= series.notna()
                v = int(neg_mask.sum())
                results.append(
                    _fail(f"filosofi.fact_menages.business.{col}_positive",
                          D, "business", "CRITICAL",
                          {"violations": v, "non_null": int(series.notna().sum())},
                          f"{v} row(s) with mediane_niveau_vie_euros <= 0.",
                          _sample(menages, neg_mask))
                    if v else
                    _pass(f"filosofi.fact_menages.business.{col}_positive",
                          D, "business", "CRITICAL",
                          {"non_null": int(series.notna().sum())},
                          f"mediane_niveau_vie_euros > 0 for all non-null values "
                          f"({int(series.notna().sum())} rows).")
                )

            # pct_menages_imposables in [0, 100] when not null
            results.append(self._range_check(
                "filosofi/fact_menages", "pct_menages_imposables", D, 0.0, 100.0, "WARNING"))

        # Poverty rates in [0, 100]
        for col in ["taux_pauvrete_ensemble", "taux_pauvrete_moins_30ans"]:
            results.append(self._range_check(
                "filosofi/fact_pauvrete", col, D, 0.0, 100.0, "WARNING"))

        # Revenus percentages in [-100, 200]  (can be negative, e.g. pct_impots)
        for col in ["pct_revenus_activite", "pct_pensions_retraites",
                    "pct_prestations_sociales"]:
            results.append(self._range_check(
                "filosofi/fact_revenus", col, D, -100.0, 200.0, "WARNING"))

        # Decile ratio: D9/D1 — should be > 1 when both populated
        deciles = self._load("filosofi/fact_deciles")
        if deciles is not None:
            d1 = pd.to_numeric(deciles.get("decile_1_revenu", pd.Series(dtype=float)),
                               errors="coerce")
            d9 = pd.to_numeric(deciles.get("decile_9_revenu", pd.Series(dtype=float)),
                               errors="coerce")
            both_valid = d1.notna() & d9.notna() & (d1 > 0)
            invalid_ratio = both_valid & (d9 < d1)
            v = int(invalid_ratio.sum())
            results.append(
                _fail("filosofi.fact_deciles.business.decile_order",
                      D, "business", "WARNING",
                      {"violations": v, "checked": int(both_valid.sum())},
                      f"{v} row(s): decile_9 < decile_1 (implausible income distribution).",
                      _sample(deciles, invalid_ratio))
                if v else
                _pass("filosofi.fact_deciles.business.decile_order",
                      D, "business", "WARNING",
                      {"checked": int(both_valid.sum())},
                      f"decile_9 >= decile_1 for all {int(both_valid.sum())} rows with both values populated.")
            )

        # INSEE secret statistique coverage documentation
        menages = self._load("filosofi/fact_menages")
        if menages is not None:
            col = "mediane_niveau_vie_euros"
            if col in menages.columns:
                null_count = int(menages[col].isnull().sum())
                n = len(menages)
                results.append(_pass(
                    "filosofi.fact_menages.coverage.secret_statistique",
                    D, "coverage", "INFO",
                    {"null_count": null_count, "total": n,
                     "null_rate_pct": _pct(null_count, n),
                     "note": "INSEE secret statistique: metrics suppressed for zones with "
                             "<11 households or <31 persons (source: INSEE diffusion thresholds)"},
                    f"FILOSOFI mediane: {null_count}/{n} values suppressed ({_pct(null_count, n):.1f}%). "
                    "Expected: INSEE applies secret statistique to protect small population zones."
                ))

        return results

    # ──────────────────────────────────────────────────────────────────────
    #  CROSS-DOMAIN
    # ──────────────────────────────────────────────────────────────────────

    def _check_cross(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        D = "cross"

        elec_communes  = self._load("election/dim_commune")
        filo_communes  = self._load("filosofi/dim_commune")
        sec_demo       = self._load("security/fact_demographie")
        part           = self._load("election/fact_participation")
        sec            = self._load("security/fact_securite")

        # ── Commune overlap across domains ────────────────────────────────
        if elec_communes is not None and filo_communes is not None:
            elec_set = set(elec_communes["id_commune"].astype(str))
            filo_set = set(filo_communes["id_commune"].astype(str))
            in_both  = elec_set & filo_set
            only_elec = elec_set - filo_set
            only_filo = filo_set - elec_set
            results.append(_pass(
                "cross.commune_overlap.election_vs_filosofi",
                D, "coverage", "INFO",
                {"election_communes": len(elec_set),
                 "filosofi_communes": len(filo_set),
                 "in_both": len(in_both),
                 "only_in_election": len(only_elec),
                 "only_in_filosofi": len(only_filo),
                 "note": "Difference expected: INSEE FILOSOFI excludes zones below diffusion thresholds"},
                f"Commune overlap election↔FILOSOFI: {len(in_both)} shared, "
                f"{len(only_elec)} election-only, {len(only_filo)} FILOSOFI-only. "
                "Gap expected due to INSEE secret statistique on small communes."
            ))

        if elec_communes is not None and sec_demo is not None:
            elec_set = set(elec_communes["id_commune"].astype(str))
            sec_set  = set(sec_demo["id_commune"].astype(str))
            in_both  = elec_set & sec_set
            results.append(_pass(
                "cross.commune_overlap.election_vs_security",
                D, "coverage", "INFO",
                {"election_communes": len(elec_set),
                 "security_communes": len(sec_set),
                 "in_both": len(in_both),
                 "coverage_pct": _pct(len(in_both), len(elec_set))},
                f"Commune overlap election↔security: {len(in_both)}/{len(elec_set)} "
                f"({_pct(len(in_both), len(elec_set)):.1f}%)."
            ))

        # ── ML inner join coverage ────────────────────────────────────────
        if filo_communes is not None and sec_demo is not None:
            filo_set = set(filo_communes["id_commune"].astype(str))
            sec_set  = set(sec_demo["id_commune"].astype(str))
            ml_set   = filo_set & sec_set
            elec_n   = len(elec_communes) if elec_communes is not None else "?"
            results.append(_pass(
                "cross.ml_inner_join_coverage",
                D, "coverage", "WARNING",
                {"filosofi_communes": len(filo_set),
                 "security_communes": len(sec_set),
                 "ml_communes": len(ml_set),
                 "election_communes": elec_n,
                 "note": "ML model uses inner join of FILOSOFI × security — small communes excluded"},
                f"ML model commune scope (FILOSOFI ∩ security inner join): {len(ml_set)} communes "
                f"out of {elec_n} election communes. "
                "Reduction expected due to FILOSOFI secret statistique exclusions."
            ))

        # ── Temporal alignment check ──────────────────────────────────────
        # Security and election temporal ranges
        if sec is not None and part is not None:
            elec_years   = sorted(self._load("election/dim_election")["annee_election"]
                                  .unique().tolist()) if self._load("election/dim_election") is not None else []
            sec_years    = sorted(sec["annee"].unique().tolist())
            filo_dim_t   = self._load("filosofi/dim_time")
            filo_years   = sorted(filo_dim_t["annee"].tolist()) if filo_dim_t is not None else []

            results.append(_pass(
                "cross.temporal_alignment",
                D, "coverage", "INFO",
                {"election_years": elec_years,
                 "security_years": sec_years,
                 "filosofi_years": filo_years,
                 "note": ("ML uses latest available year of features (security: max year, "
                          "FILOSOFI: 2021) applied retrospectively to 2012/2017/2022 elections. "
                          "Known temporal leakage — documented as POC limitation.")},
                f"Temporal ranges — Elections: {elec_years}, Security: {sec_years[0]}–{sec_years[-1]}, "
                f"FILOSOFI: {filo_years[0] if filo_years else '?'}–{filo_years[-1] if filo_years else '?'}. "
                "ML features use latest year (temporal leakage documented as known POC limitation)."
            ))

        # ── Gold layer freshness ──────────────────────────────────────────
        if part is not None and "date_integration_gold" in part.columns:
            dates = pd.to_datetime(part["date_integration_gold"], errors="coerce")
            latest = dates.max()
            if pd.notna(latest):
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                age_days = (now - latest.replace(tzinfo=None)).days
                results.append(
                    _warn("cross.gold_freshness.election",
                          D, "coverage", "WARNING",
                          {"last_integration": str(latest), "age_days": age_days},
                          f"Gold election layer last updated {age_days} days ago ({latest.date()}). "
                          "Consider refreshing if source data has been updated.")
                    if age_days > 90 else
                    _pass("cross.gold_freshness.election",
                          D, "coverage", "INFO",
                          {"last_integration": str(latest), "age_days": age_days},
                          f"Gold election layer updated {age_days} days ago ({latest.date()}) — fresh.")
                )

        # ── Bronze → Gold row volume sanity ──────────────────────────────
        bronze_dir = self.root / "data" / "bronze"
        if bronze_dir.exists():
            bronze_csvs = list(bronze_dir.glob("*.csv"))
            results.append(_pass(
                "cross.bronze_layer_present",
                D, "schema", "INFO",
                {"bronze_files": len(bronze_csvs),
                 "files": [f.name for f in bronze_csvs]},
                f"Bronze layer contains {len(bronze_csvs)} CSV source file(s)."
            ))

        silver_dir = self.root / "data" / "silver"
        if silver_dir.exists():
            silver_csvs = list(silver_dir.glob("*.csv"))
            results.append(_pass(
                "cross.silver_layer_present",
                D, "schema", "INFO",
                {"silver_files": len(silver_csvs),
                 "files": [f.name for f in silver_csvs]},
                f"Silver layer contains {len(silver_csvs)} CSV intermediate file(s)."
            ))

        # Gold file count
        gold_dir = self.root / "data" / "gold"
        if gold_dir.exists():
            gold_csvs = list(gold_dir.rglob("*.csv"))
            results.append(_pass(
                "cross.gold_layer_present",
                D, "schema", "INFO",
                {"gold_files": len(gold_csvs),
                 "files": [f.relative_to(gold_dir).as_posix() for f in gold_csvs]},
                f"Gold layer contains {len(gold_csvs)} CSV table(s) across all domains."
            ))

        return results
