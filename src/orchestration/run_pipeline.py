#!/usr/bin/env python3
"""
Electio Analytics — End-to-End Pipeline Orchestrator
=====================================================
Executes every notebook in the correct dependency order:

  Stage 1 — ingestion     : RAW  → Bronze  (downloads source files, internet required)
  Stage 2 — transformation: Bronze → Silver (cleans and normalises)
  Stage 3 — gold          : Silver → Gold   (builds star-schema dimensions & facts)
  Stage 4 — quality       : Gold  → Report  (data quality checks)

Usage
-----
  # Full pipeline (all stages)
  python3 src/orchestration/run_pipeline.py

  # Skip ingestion (bronze files already present)
  python3 src/orchestration/run_pipeline.py --start-from transformation

  # Only rebuild gold
  python3 src/orchestration/run_pipeline.py --stages gold

  # Gold + quality
  python3 src/orchestration/run_pipeline.py --stages gold quality

  # Dry-run: show what would be executed without running anything
  python3 src/orchestration/run_pipeline.py --dry-run

Exit codes
----------
  0 — all requested stages completed successfully
  1 — at least one notebook or quality check failed
  2 — configuration error (project root not found, missing prerequisite)
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
#  Project root resolution
# ─────────────────────────────────────────────────────────────────────────────

def _find_root() -> pathlib.Path:
    here = pathlib.Path(__file__).resolve().parent
    candidate = here
    while candidate != candidate.parent:
        if (candidate / "data" / "gold").is_dir():
            return candidate
        candidate = candidate.parent
    raise RuntimeError(
        "Cannot locate project root (directory containing data/gold/). "
        "Run this script from inside the electio-analytics-poc repository."
    )

ROOT = _find_root()
SRC  = ROOT / "src"

# ─────────────────────────────────────────────────────────────────────────────
#  Colours (disabled on non-TTY)
# ─────────────────────────────────────────────────────────────────────────────

_USE_COLOUR = sys.stdout.isatty()

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOUR else text

def green(t):  return _c("32", t)
def red(t):    return _c("31", t)
def yellow(t): return _c("33", t)
def cyan(t):   return _c("36", t)
def bold(t):   return _c("1",  t)
def dim(t):    return _c("2",  t)


# ─────────────────────────────────────────────────────────────────────────────
#  Data structures
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NotebookSpec:
    """Full specification of a pipeline notebook."""
    stage: str                         # ingestion | transformation | gold | quality
    key: str                           # unique human key
    path: pathlib.Path                 # absolute path to .ipynb
    description: str
    prerequisites: list[pathlib.Path]  # input files that MUST exist before running
    outputs: list[pathlib.Path]        # files that MUST exist after running
    network_required: bool = False     # True → needs internet to download source data
    timeout: int = 600                 # nbconvert kernel timeout in seconds


@dataclass
class StepResult:
    key: str
    status: str          # PASS | FAIL | SKIP | PREREQ_MISSING
    duration_s: float = 0.0
    error: Optional[str] = None
    missing_prereqs: list[str] = field(default_factory=list)
    missing_outputs: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
#  Pipeline definition
#  Order within each stage = execution order
# ─────────────────────────────────────────────────────────────────────────────

def _nb(relative: str) -> pathlib.Path:
    """Resolve a notebook path relative to SRC."""
    return SRC / relative


def _d(*parts: str) -> pathlib.Path:
    """Resolve a data path relative to ROOT."""
    return ROOT / "data" / pathlib.Path(*parts)


PIPELINE: list[NotebookSpec] = [

    # ── STAGE 1: INGESTION ────────────────────────────────────────────────

    NotebookSpec(
        stage="ingestion",
        key="ingest_nuance",
        path=_nb("ingestion/nuance politique/dictionnaire des nuances politiques.ipynb"),
        description="Ingest political nuance dictionary (reference table)",
        prerequisites=[
            _d("raw", "dictionnaire des nuances politiques.csv"),
        ],
        outputs=[
            _d("bronze", "bronze_dictionnaire_des_nuances_politiques.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_2012",
        path=_nb("ingestion/2012_pres t1-2/2012-pres-t1-2-commune.ipynb"),
        description="Ingest 2012 presidential elections T1 & T2 → Bronze",
        prerequisites=[
            _d("raw", "2012_pres_t1_t2_communes_france.xls"),
        ],
        outputs=[
            _d("bronze", "2012-pres-t1-commune-rhone-69-bronze.csv"),
            _d("bronze", "2012-pres-t2-commune-rhone-69-bronze.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_2017_t1",
        path=_nb("ingestion/2017_pres t1/2017-pres-t1-commune.ipynb"),
        description="Ingest 2017 presidential election T1 → Bronze",
        prerequisites=[
            _d("raw", "PR17_BVot_T1_FE.txt"),
        ],
        outputs=[
            _d("bronze", "2017-pres-t1-commune-rhone-69-bronze.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_2017_t2",
        path=_nb("ingestion/2017_pres t2/2017-pres-t2-commune.ipynb"),
        description="Ingest 2017 presidential election T2 → Bronze",
        prerequisites=[
            _d("raw", "PR17_BVot_T2_FE.txt"),
        ],
        outputs=[
            _d("bronze", "2017-pres-t2-commune-rhone-69-bronze.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_2022_t1",
        path=_nb("ingestion/2022_pres t1/2022-pres-t1-commune.ipynb"),
        description="Ingest 2022 presidential election T1 → Bronze",
        prerequisites=[
            _d("raw", "2022_burvot_t1_france_entiere.xlsx"),
        ],
        outputs=[
            _d("bronze", "2022-pres-t1-commune-rhone-69-bronze.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_2022_t2",
        path=_nb("ingestion/2022_pres t2/2022-pres-t2-commune.ipynb"),
        description="Ingest 2022 presidential election T2 → Bronze",
        prerequisites=[
            _d("raw", "resultats-par-niveau-subcom-t2-france-entiere.xlsx"),
        ],
        outputs=[
            _d("bronze", "2022-pres-t2-commune-rhone-69-bronze.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_dep",
        path=_nb("ingestion/DEP-departementale/DEP-departementale.ipynb"),
        description="Ingest departmental security data (data.gouv.fr) → Bronze",
        prerequisites=[
            _d("raw", "DEP-departementale.csv"),
        ],
        outputs=[
            _d("bronze", "bronze_DEP-departementale.csv"),
        ],
        network_required=False,
    ),

    NotebookSpec(
        stage="ingestion",
        key="ingest_filosofi",
        path=_nb("ingestion/filosofi-14-21/filosofi-14-21.ipynb"),
        description="Ingest INSEE FILOSOFI income/poverty data 2014–2021 → Bronze (downloads ZIPs)",
        prerequisites=[],   # downloads directly from INSEE — no local prerequisite
        outputs=[
            _d("bronze", "filosofi_2014_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2015_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2016_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2017_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2018_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2019_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2020_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2021_rhone_69_bronze.csv"),
        ],
        network_required=True,
        timeout=900,
    ),

    # ── STAGE 2: TRANSFORMATION ───────────────────────────────────────────

    NotebookSpec(
        stage="transformation",
        key="transform_nuance",
        path=_nb("transformation/nuance politique/dictionnaire des nuances politiques.ipynb"),
        description="Transform nuance dictionary Bronze → Silver reference table",
        prerequisites=[
            _d("bronze", "bronze_dictionnaire_des_nuances_politiques.csv"),
        ],
        outputs=[
            _d("silver", "dictionnaire_des_nuances_politiques_silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_2012_t1",
        path=_nb("transformation/2012-pre/2012-pres-t1-commune.ipynb"),
        description="Transform 2012 T1 Bronze → Silver (normalise + nuance join)",
        prerequisites=[
            _d("bronze", "2012-pres-t1-commune-rhone-69-bronze.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("silver", "2012-pres-t1-commune-rhone-69-silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_2012_t2",
        path=_nb("transformation/2012-pre/2012-pres-t2-commune.ipynb"),
        description="Transform 2012 T2 Bronze → Silver",
        prerequisites=[
            _d("bronze", "2012-pres-t2-commune-rhone-69-bronze.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("silver", "2012-pres-t2-commune-rhone-69-silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_2017_t1",
        path=_nb("transformation/2017-pre/2017-pres-t1-commune.ipynb"),
        description="Transform 2017 T1 Bronze → Silver",
        prerequisites=[
            _d("bronze", "2017-pres-t1-commune-rhone-69-bronze.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("silver", "2017-pres-t1-commune-rhone-69-silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_2017_t2",
        path=_nb("transformation/2017-pre/2017-pres-t2-commune.ipynb"),
        description="Transform 2017 T2 Bronze → Silver",
        prerequisites=[
            _d("bronze", "2017-pres-t2-commune-rhone-69-bronze.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("silver", "2017-pres-t2-commune-rhone-69-silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_2022_t1",
        path=_nb("transformation/2022-pre/2022-pres-t1-commune.ipynb"),
        description="Transform 2022 T1 Bronze → Silver",
        prerequisites=[
            _d("bronze", "2022-pres-t1-commune-rhone-69-bronze.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("silver", "2022-pres-t1-commune-rhone-69-silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_2022_t2",
        path=_nb("transformation/2022-pre/2022-pres-t2-commune.ipynb"),
        description="Transform 2022 T2 Bronze → Silver",
        prerequisites=[
            _d("bronze", "2022-pres-t2-commune-rhone-69-bronze.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("silver", "2022-pres-t2-commune-rhone-69-silver.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_dep",
        path=_nb("transformation/DEP-departementale/DEP-departementale.ipynb"),
        description="Transform departmental security data Bronze → Silver",
        prerequisites=[
            _d("bronze", "bronze_DEP-departementale.csv"),
        ],
        outputs=[
            _d("silver", "silver_DEP-departementale.csv"),
        ],
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_filosofi",
        path=_nb("transformation/filosofi/filosofi_bronze_to_silver.ipynb"),
        description="Transform FILOSOFI 2014–2021 Bronze → Silver (merge + unpivot)",
        prerequisites=[
            _d("bronze", "filosofi_2014_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2015_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2016_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2017_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2018_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2019_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2020_rhone_69_bronze.csv"),
            _d("bronze", "filosofi_2021_rhone_69_bronze.csv"),
        ],
        outputs=[
            _d("silver", "filosofi_2014_2021_commune_silver.csv"),
        ],
        timeout=300,
    ),

    NotebookSpec(
        stage="transformation",
        key="transform_securite",
        path=_nb("transformation/securite/16-24-security.ipynb"),
        description="Transform commune security data → Silver (clean + type-cast)",
        prerequisites=[
            _d("silver", "RAYAN securite_data_silver.csv"),
        ],
        outputs=[
            _d("silver", "securite_data_silver_clean.csv"),
        ],
    ),

    # ── STAGE 3: GOLD ─────────────────────────────────────────────────────

    NotebookSpec(
        stage="gold",
        key="gold_election",
        path=_nb("orchestration/election_build_gold_layer.ipynb"),
        description="Build election gold star schema (dims + facts) → data/gold/election/",
        prerequisites=[
            _d("silver", "2012-pres-t1-commune-rhone-69-silver.csv"),
            _d("silver", "2012-pres-t2-commune-rhone-69-silver.csv"),
            _d("silver", "2017-pres-t1-commune-rhone-69-silver.csv"),
            _d("silver", "2017-pres-t2-commune-rhone-69-silver.csv"),
            _d("silver", "2022-pres-t1-commune-rhone-69-silver.csv"),
            _d("silver", "2022-pres-t2-commune-rhone-69-silver.csv"),
            _d("reference", "nuance_politique_candidates_master.csv"),
        ],
        outputs=[
            _d("gold", "election", "dim_election.csv"),
            _d("gold", "election", "dim_commune.csv"),
            _d("gold", "election", "dim_candidat.csv"),
            _d("gold", "election", "dim_nuance.csv"),
            _d("gold", "election", "fact_participation.csv"),
            _d("gold", "election", "fact_resultats_candidat.csv"),
        ],
        timeout=300,
    ),

    NotebookSpec(
        stage="gold",
        key="gold_filosofi",
        path=_nb("orchestration/filosofi_build_gold_layer.ipynb"),
        description="Build FILOSOFI gold star schema (dims + facts) → data/gold/filosofi/",
        prerequisites=[
            _d("silver", "filosofi_2014_2021_commune_silver.csv"),
        ],
        outputs=[
            _d("gold", "filosofi", "dim_time.csv"),
            _d("gold", "filosofi", "dim_commune.csv"),
            _d("gold", "filosofi", "fact_menages.csv"),
            _d("gold", "filosofi", "fact_revenus.csv"),
            _d("gold", "filosofi", "fact_pauvrete.csv"),
            _d("gold", "filosofi", "fact_deciles.csv"),
        ],
        timeout=300,
    ),

    NotebookSpec(
        stage="gold",
        key="gold_security",
        path=_nb("orchestration/securoty-merged.ipynb"),
        description="Build security gold star schema (dims + facts) → data/gold/security/",
        prerequisites=[
            _d("silver", "securite_data_silver_clean.csv"),
            _d("silver", "silver_DEP-departementale.csv"),
        ],
        outputs=[
            _d("gold", "security", "dim_indicateur_securite.csv"),
            _d("gold", "security", "dim_departement.csv"),
            _d("gold", "security", "fact_securite.csv"),
            _d("gold", "security", "fact_securite_dep.csv"),
            _d("gold", "security", "fact_demographie.csv"),
            _d("gold", "security", "fact_demographie_dep.csv"),
        ],
        timeout=300,
    ),
]

STAGE_ORDER = ["ingestion", "transformation", "gold", "quality"]


# ─────────────────────────────────────────────────────────────────────────────
#  Logger
# ─────────────────────────────────────────────────────────────────────────────

class Logger:
    def __init__(self, log_path: pathlib.Path):
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(log_path, "w", encoding="utf-8")
        self._log_path = log_path

    def _ts(self) -> str:
        return datetime.now(timezone.utc).strftime("%H:%M:%S")

    def _write(self, line: str):
        self._fh.write(line + "\n")
        self._fh.flush()

    def info(self, msg: str):
        line = f"[{self._ts()}] INFO  {msg}"
        print(line)
        self._write(line)

    def ok(self, msg: str):
        line = f"[{self._ts()}] OK    {msg}"
        print(green(line))
        self._write(line)

    def warn(self, msg: str):
        line = f"[{self._ts()}] WARN  {msg}"
        print(yellow(line))
        self._write(line)

    def error(self, msg: str):
        line = f"[{self._ts()}] ERROR {msg}"
        print(red(line))
        self._write(line)

    def section(self, title: str):
        sep = "─" * 65
        line = f"\n{sep}\n  {title}\n{sep}"
        print(bold(cyan(line)))
        self._write(line)

    def close(self):
        self._fh.close()

    @property
    def path(self):
        return self._log_path


# ─────────────────────────────────────────────────────────────────────────────
#  PipelineRunner
# ─────────────────────────────────────────────────────────────────────────────

class PipelineRunner:

    def __init__(
        self,
        stages: list[str],
        dry_run: bool = False,
        skip_output_check: bool = False,
    ):
        self.stages = stages
        self.dry_run = dry_run
        self.skip_output_check = skip_output_check

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        log_dir = ROOT / "data" / "pipeline_logs"
        self.log = Logger(log_dir / f"{ts}_pipeline.log")
        self.results: list[StepResult] = []

        # path to the correct jupyter in the venv
        self._jupyter = self._find_jupyter()
        # prefer the kernelspec that is wired to this repository's .venv
        self._kernel_name = self._find_kernel_name()

    def _find_jupyter(self) -> str:
        """Prefer venv jupyter; fall back to system jupyter."""
        candidates = [
            str(ROOT / ".venv" / "bin" / "jupyter"),
            "jupyter",
        ]
        for c in candidates:
            try:
                subprocess.run(
                    [c, "--version"],
                    capture_output=True, check=True,
                )
                return c
            except (subprocess.CalledProcessError, FileNotFoundError):
                continue
        raise RuntimeError(
            "jupyter not found. Install it: pip install jupyter nbconvert"
        )

    def _kernel_uses_python(
        self,
        kernelspec_meta: dict,
        expected_python: pathlib.Path,
    ) -> bool:
        """Return True when a kernelspec launches the expected Python interpreter."""
        resource_dir = kernelspec_meta.get("resource_dir")
        if not resource_dir:
            return False

        kernel_json = pathlib.Path(resource_dir) / "kernel.json"
        if not kernel_json.exists():
            return False

        try:
            payload = json.loads(kernel_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False

        argv = payload.get("argv") or []
        if not argv:
            return False

        kernel_python = pathlib.Path(argv[0])
        try:
            return kernel_python.resolve() == expected_python.resolve()
        except OSError:
            return kernel_python == expected_python

    def _find_kernel_name(self) -> str:
        """
        Prefer the kernelspec bound to this repository's .venv.

        The previous hardcoded `python3` choice could resolve to a system kernel,
        which caused notebooks to run outside the project virtualenv.
        """
        venv_python = ROOT / ".venv" / "bin" / "python"
        if not venv_python.exists():
            return "python3"

        try:
            proc = subprocess.run(
                [self._jupyter, "kernelspec", "list", "--json"],
                capture_output=True,
                text=True,
                check=True,
            )
            kernelspecs = json.loads(proc.stdout).get("kernelspecs", {})
        except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError):
            kernelspecs = {}

        preferred_names = [ROOT.name, "python3"]
        for name in preferred_names:
            meta = kernelspecs.get(name)
            if meta and self._kernel_uses_python(meta, venv_python):
                return name

        for name, meta in kernelspecs.items():
            if self._kernel_uses_python(meta, venv_python):
                return name

        return "python3"

    # ── prerequisite / output checks ──────────────────────────────────────

    def _check_prerequisites(self, spec: NotebookSpec) -> list[str]:
        """Return list of missing prerequisite paths (empty = all good)."""
        missing = []
        for p in spec.prerequisites:
            if not p.exists():
                missing.append(str(p.relative_to(ROOT)))
        return missing

    def _check_outputs(self, spec: NotebookSpec) -> list[str]:
        """Return list of missing output paths (empty = all good)."""
        missing = []
        for p in spec.outputs:
            if not p.exists():
                missing.append(str(p.relative_to(ROOT)))
        return missing

    # ── notebook execution ─────────────────────────────────────────────────

    def _run_notebook(self, spec: NotebookSpec) -> StepResult:
        nb_path = spec.path
        nb_dir  = nb_path.parent

        if not nb_path.exists():
            return StepResult(
                key=spec.key,
                status="FAIL",
                error=f"Notebook not found: {nb_path.relative_to(ROOT)}",
            )

        # Check prerequisites
        missing_pre = self._check_prerequisites(spec)
        if missing_pre:
            return StepResult(
                key=spec.key,
                status="PREREQ_MISSING",
                missing_prereqs=missing_pre,
            )

        if self.dry_run:
            return StepResult(key=spec.key, status="SKIP")

        cmd = [
            self._jupyter, "nbconvert",
            "--to", "notebook",
            "--execute",
            f"--ExecutePreprocessor.timeout={spec.timeout}",
            f"--ExecutePreprocessor.kernel_name={self._kernel_name}",
            "--inplace",
            str(nb_path),
        ]

        t0 = time.perf_counter()
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(nb_dir),
                capture_output=True,
                text=True,
            )
            duration = time.perf_counter() - t0

            if proc.returncode != 0:
                return StepResult(
                    key=spec.key,
                    status="FAIL",
                    duration_s=duration,
                    error=(proc.stderr or proc.stdout or "nbconvert returned non-zero exit")[-2000:],
                )

        except Exception as exc:
            return StepResult(
                key=spec.key,
                status="FAIL",
                duration_s=time.perf_counter() - t0,
                error=str(exc),
            )

        # Verify outputs were produced
        missing_out = [] if self.skip_output_check else self._check_outputs(spec)
        return StepResult(
            key=spec.key,
            status="FAIL" if missing_out else "PASS",
            duration_s=duration,
            missing_outputs=missing_out,
            error=(f"Notebook finished but missing outputs: {missing_out}"
                   if missing_out else None),
        )

    # ── quality stage ──────────────────────────────────────────────────────

    def _run_quality(self) -> StepResult:
        self.log.info("Running data quality checks …")
        quality_script = SRC / "quality" / "run_quality.py"

        if not quality_script.exists():
            return StepResult(
                key="quality",
                status="FAIL",
                error=f"Quality script not found: {quality_script.relative_to(ROOT)}",
            )

        python = str(ROOT / ".venv" / "bin" / "python3")
        if not pathlib.Path(python).exists():
            python = sys.executable

        t0 = time.perf_counter()
        if self.dry_run:
            return StepResult(key="quality", status="SKIP")

        proc = subprocess.run(
            [python, str(quality_script)],
            cwd=str(ROOT),
            capture_output=False,   # stream output live
            text=True,
        )
        duration = time.perf_counter() - t0
        status = "PASS" if proc.returncode == 0 else "FAIL"
        return StepResult(
            key="quality",
            status=status,
            duration_s=duration,
            error=None if status == "PASS" else f"Quality checks exited with code {proc.returncode}",
        )

    # ── stage runner ───────────────────────────────────────────────────────

    def _run_stage(self, stage: str) -> bool:
        """Run all notebooks for a given stage. Returns True if all passed."""
        if stage == "quality":
            self.log.section(f"STAGE: QUALITY")
            result = self._run_quality()
            self.results.append(result)
            self._log_result(result, "Quality checks")
            return result.status in ("PASS", "SKIP")

        notebooks = [s for s in PIPELINE if s.stage == stage]
        if not notebooks:
            self.log.warn(f"No notebooks defined for stage '{stage}'.")
            return True

        self.log.section(f"STAGE: {stage.upper()}  ({len(notebooks)} notebook(s))")
        all_ok = True

        for spec in notebooks:
            self.log.info(f"  ▶ {spec.key}  —  {spec.description}")
            if spec.network_required and not self.dry_run:
                self.log.warn("    ⚠️  Network access required for this notebook.")

            result = self._run_notebook(spec)
            self.results.append(result)
            self._log_result(result, spec.path.name)

            if result.status not in ("PASS", "SKIP"):
                all_ok = False
                if result.status == "PREREQ_MISSING":
                    self.log.error(
                        f"    Prerequisite file(s) missing — skipping downstream steps "
                        f"that depend on this notebook's outputs."
                    )

        return all_ok

    def _log_result(self, result: StepResult, label: str):
        t = f"{result.duration_s:.1f}s" if result.duration_s else ""
        if result.status == "PASS":
            self.log.ok(f"    ✅ {label}  {t}")
        elif result.status == "SKIP":
            self.log.info(f"    ⏭️  {label}  (dry-run)")
        elif result.status == "PREREQ_MISSING":
            self.log.warn(
                f"    ⏭️  {label}  PREREQ_MISSING: {result.missing_prereqs}"
            )
        else:
            self.log.error(f"    ❌ {label}  {t}")
            if result.error:
                for line in result.error.splitlines()[-10:]:
                    self.log.error(f"       {line}")
            if result.missing_outputs:
                self.log.error(f"       Missing outputs: {result.missing_outputs}")

    # ── main entry point ───────────────────────────────────────────────────

    def run(self) -> int:
        """Execute all requested stages. Returns exit code (0 = success)."""
        started = datetime.now(timezone.utc)
        self.log.section(
            f"ELECTIO ANALYTICS PIPELINE  —  {started.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        self.log.info(f"  Project root : {ROOT}")
        self.log.info(f"  Stages       : {self.stages}")
        self.log.info(f"  Dry-run      : {self.dry_run}")
        self.log.info(f"  Kernel       : {self._kernel_name}")
        self.log.info(f"  Log file     : {self.log.path}")

        all_ok = True
        for stage in self.stages:
            ok = self._run_stage(stage)
            if not ok:
                all_ok = False

        self._write_summary(started, all_ok)
        self.log.close()
        return 0 if all_ok else 1

    def _write_summary(self, started: datetime, all_ok: bool):
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        passed  = sum(1 for r in self.results if r.status == "PASS")
        failed  = sum(1 for r in self.results if r.status == "FAIL")
        skipped = sum(1 for r in self.results if r.status in ("SKIP", "PREREQ_MISSING"))
        total   = len(self.results)

        self.log.section("PIPELINE SUMMARY")
        self.log.info(f"  Total steps : {total}")
        self.log.info(f"  ✅ Passed   : {passed}")
        self.log.info(f"  ❌ Failed   : {failed}")
        self.log.info(f"  ⏭️  Skipped  : {skipped}")
        self.log.info(f"  ⏱  Duration : {elapsed:.1f}s")

        if self.dry_run:
            self.log.ok("  DRY-RUN COMPLETE — no notebooks were executed")
        elif all_ok:
            self.log.ok("  PIPELINE COMPLETED SUCCESSFULLY")
        else:
            self.log.error("  PIPELINE COMPLETED WITH FAILURES")
            self.log.error("  Failed steps:")
            for r in self.results:
                if r.status == "FAIL":
                    self.log.error(f"    - {r.key}: {r.error or 'unknown error'}")

        # Write machine-readable JSON summary alongside the log
        summary_path = self.log.path.with_suffix(".json")
        summary = {
            "started_at":  started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_s":  round(elapsed, 2),
            "stages":      self.stages,
            "dry_run":     self.dry_run,
            "status":      "SUCCESS" if all_ok else "FAILURE",
            "steps": [
                {
                    "key":              r.key,
                    "status":           r.status,
                    "duration_s":       round(r.duration_s, 2),
                    "error":            r.error,
                    "missing_prereqs":  r.missing_prereqs,
                    "missing_outputs":  r.missing_outputs,
                }
                for r in self.results
            ],
        }
        with open(summary_path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, ensure_ascii=False)
        self.log.info(f"  JSON summary : {summary_path}")


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="run_pipeline.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--stages",
        nargs="+",
        choices=STAGE_ORDER,
        metavar="STAGE",
        help=(
            "Run only the specified stage(s). "
            "Choices: ingestion transformation gold quality. "
            "Default: all four stages."
        ),
    )
    p.add_argument(
        "--start-from",
        choices=STAGE_ORDER,
        metavar="STAGE",
        dest="start_from",
        help="Run from the specified stage through to quality (inclusive).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the execution plan without running any notebooks.",
    )
    p.add_argument(
        "--skip-output-check",
        action="store_true",
        dest="skip_output_check",
        help="Do not verify that output files were created after each notebook.",
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="List all pipeline steps and exit.",
    )
    return p.parse_args()


def _list_pipeline():
    """Print a human-readable table of all pipeline steps."""
    current_stage = None
    print()
    for spec in PIPELINE:
        if spec.stage != current_stage:
            current_stage = spec.stage
            print(bold(f"\n  ── {current_stage.upper()} ──"))
        prereqs = ", ".join(p.name for p in spec.prerequisites) or "none"
        outputs = ", ".join(p.name for p in spec.outputs)
        net     = " 🌐" if spec.network_required else ""
        print(f"  {cyan(spec.key)}{net}")
        print(f"    {dim(spec.description)}")
        print(f"    prereqs : {dim(prereqs)}")
        print(f"    outputs : {dim(outputs)}")
    print(f"\n  quality stage runs src/quality/run_quality.py\n")


def main() -> int:
    args = parse_args()

    if args.list:
        _list_pipeline()
        return 0

    # Determine which stages to run
    if args.stages:
        stages = args.stages
    elif args.start_from:
        idx    = STAGE_ORDER.index(args.start_from)
        stages = STAGE_ORDER[idx:]
    else:
        stages = STAGE_ORDER  # all four

    runner = PipelineRunner(
        stages=stages,
        dry_run=args.dry_run,
        skip_output_check=args.skip_output_check,
    )
    return runner.run()


if __name__ == "__main__":
    sys.exit(main())
