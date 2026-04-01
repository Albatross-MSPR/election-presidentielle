"""
Report generation for Electio Analytics quality checks.

Produces two artefacts:
  <timestamp>_quality_report.json   — machine-readable full detail
  <timestamp>_quality_report.md     — human-readable summary (for evaluators)
"""

from __future__ import annotations

import json
import pathlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .checks import CheckResult

# ─────────────────────────────────────────────────────────────────────────────
#  Status / severity icons
# ─────────────────────────────────────────────────────────────────────────────

STATUS_ICON = {
    "PASS": "✅",
    "FAIL": "❌",
    "WARN": "⚠️ ",
    "SKIP": "⏭️ ",
}

SEV_ORDER = {"CRITICAL": 0, "WARNING": 1, "INFO": 2}
STATUS_ORDER = {"FAIL": 0, "WARN": 1, "SKIP": 2, "PASS": 3}


# ─────────────────────────────────────────────────────────────────────────────
#  Summary builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary(results: list) -> dict:
    total    = len(results)
    passed   = sum(1 for r in results if r.status == "PASS")
    failed   = sum(1 for r in results if r.status == "FAIL")
    warned   = sum(1 for r in results if r.status == "WARN")
    skipped  = sum(1 for r in results if r.status == "SKIP")
    critical = sum(1 for r in results if r.status == "FAIL" and r.severity == "CRITICAL")

    by_domain: dict[str, dict] = defaultdict(lambda: {
        "total": 0, "passed": 0, "failed": 0, "warned": 0, "skipped": 0
    })
    for r in results:
        d = by_domain[r.domain]
        d["total"] += 1
        key = r.status.lower() + "ed" if r.status not in ("PASS", "SKIP") else (
            "passed" if r.status == "PASS" else "skipped"
        )
        # normalise key names
        key_map = {"passed": "passed", "failed": "failed",
                   "warned": "warned", "skipped": "skipped"}
        d[key_map.get(key, key)] += 1

    by_category: dict[str, int] = defaultdict(int)
    for r in results:
        by_category[r.category] += 1

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "skipped": skipped,
        "critical_failures": critical,
        "health_score": round(passed / total * 100, 1) if total else 0.0,
        "by_domain": dict(by_domain),
        "by_category": dict(by_category),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  JSON serialiser
# ─────────────────────────────────────────────────────────────────────────────

def _result_to_dict(r) -> dict:
    return {
        "name":     r.name,
        "domain":   r.domain,
        "category": r.category,
        "severity": r.severity,
        "status":   r.status,
        "metric":   r.metric,
        "details":  r.details,
        "sample":   r.sample,
    }


def generate_report(
    results: list,
    output_dir: pathlib.Path,
    project_name: str = "Electio Analytics",
) -> tuple[pathlib.Path, pathlib.Path]:
    """
    Write JSON + Markdown reports to output_dir.
    Returns (json_path, md_path).
    """
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ts    = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    ts_hr = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    summary = _build_summary(results)

    # ── JSON ──────────────────────────────────────────────────────────────
    payload = {
        "generated_at": ts_hr,
        "project":      project_name,
        "summary":      summary,
        "checks":       [_result_to_dict(r) for r in results],
    }
    json_path = output_dir / f"{ts}_quality_report.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)

    # ── Markdown ──────────────────────────────────────────────────────────
    md_path = output_dir / f"{ts}_quality_report.md"
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(_build_markdown(results, summary, ts_hr, project_name))

    return json_path, md_path


# ─────────────────────────────────────────────────────────────────────────────
#  Markdown builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_markdown(results: list, summary: dict, ts: str, project: str) -> str:
    lines: list[str] = []
    w = lines.append

    # ── Header ──────────────────────────────────────────────────────────
    w(f"# Data Quality Report — {project}")
    w(f"**Generated:** {ts}  ")
    w(f"**Total checks:** {summary['total']}  ")
    w("")

    # ── Global summary ────────────────────────────────────────────────
    w("## Summary")
    w("")
    w(f"| Status | Count |")
    w(f"|--------|-------|")
    w(f"| ✅ PASS   | {summary['passed']} |")
    w(f"| ❌ FAIL   | {summary['failed']} |")
    w(f"| ⚠️  WARN  | {summary['warned']} |")
    w(f"| ⏭️  SKIP  | {summary['skipped']} |")
    w(f"| 🔴 CRITICAL failures | {summary['critical_failures']} |")
    w("")
    w(f"**Data health score: {summary['health_score']:.1f}%** "
      f"(PASS / total checks)")
    w("")

    # ── Per-domain table ────────────────────────────────────────────────
    w("## Results by Domain")
    w("")
    w("| Domain | Total | ✅ Pass | ❌ Fail | ⚠️  Warn | ⏭️  Skip |")
    w("|--------|-------|--------|--------|---------|---------|")
    domain_order = ["election", "security", "filosofi", "cross"]
    for domain in domain_order:
        d = summary["by_domain"].get(domain, {})
        if not d:
            continue
        w(f"| {domain.capitalize()} | {d.get('total',0)} | "
          f"{d.get('passed',0)} | {d.get('failed',0)} | "
          f"{d.get('warned',0)} | {d.get('skipped',0)} |")
    w("")

    # ── Per-category table ───────────────────────────────────────────────
    w("## Results by Category")
    w("")
    w("| Category | Checks run |")
    w("|----------|-----------|")
    for cat, n in sorted(summary["by_category"].items()):
        w(f"| {cat} | {n} |")
    w("")

    # ── Detailed results per domain ───────────────────────────────────
    for domain in domain_order:
        domain_results = [r for r in results if r.domain == domain]
        if not domain_results:
            continue

        w(f"## {domain.capitalize()} Domain")
        w("")

        # Critical failures first
        critical_fails = [r for r in domain_results
                          if r.status == "FAIL" and r.severity == "CRITICAL"]
        if critical_fails:
            w("### 🔴 Critical Failures")
            w("")
            for r in critical_fails:
                w(f"**`{r.name}`**  ")
                w(f"{r.details}  ")
                if r.metric:
                    w(f"*Metric:* `{json.dumps(r.metric, default=str)}`  ")
                if r.sample:
                    w(f"*Sample rows:*")
                    w("```")
                    for row in r.sample[:3]:
                        w(str(row))
                    w("```")
                w("")

        # Warnings
        warns = [r for r in domain_results if r.status in ("FAIL", "WARN")
                 and r.severity != "CRITICAL"]
        warns += [r for r in domain_results if r.status == "WARN"]
        warns = list({r.name: r for r in warns}.values())  # deduplicate
        if warns:
            w("### ⚠️  Warnings")
            w("")
            for r in sorted(warns, key=lambda x: STATUS_ORDER[x.status]):
                icon = STATUS_ICON[r.status]
                w(f"- {icon} **`{r.name}`** — {r.details}")
            w("")

        # Skipped
        skips = [r for r in domain_results if r.status == "SKIP"]
        if skips:
            w("### ⏭️  Skipped")
            w("")
            for r in skips:
                w(f"- ⏭️  **`{r.name}`** — {r.details}")
            w("")

        # All checks (compact table)
        w("### All Checks")
        w("")
        w("| Status | Severity | Category | Check | Details |")
        w("|--------|----------|----------|-------|---------|")
        for r in sorted(domain_results,
                        key=lambda x: (STATUS_ORDER[x.status], SEV_ORDER.get(x.severity, 9))):
            short_name = r.name.split(".")[-1]  # last segment only for readability
            detail_short = (r.details[:90] + "…") if len(r.details) > 90 else r.details
            w(f"| {STATUS_ICON[r.status]} {r.status} | {r.severity} | {r.category} "
              f"| `{short_name}` | {detail_short} |")
        w("")

    # ── Methodology notes ────────────────────────────────────────────────
    w("## Methodology Notes")
    w("")
    w("### INSEE Secret Statistique")
    w("INSEE applies statistical secrecy (secret statistique) to suppress individual")
    w("indicator values in zones with fewer than 11 households or 31 persons.")
    w("Affected rows are flagged with `est_diffuse = 'ndiff'` in `fact_securite`.")
    w("Null values in FILOSOFI tables for small communes are an expected consequence")
    w("of this legal obligation, not a data quality defect.")
    w("")
    w("### Commune Fusions (2017 → 2022)")
    w("17 communes in the Rhône department were absorbed into neighbouring communes")
    w("between 2017 and 2022 (primarily in the Beaujolais wine region).")
    w("4 new communes were created from mergers. This explains why")
    w("`fact_participation` has fewer rows than `dim_commune × dim_election`.")
    w("")
    w("### Temporal Leakage in ML Features")
    w("The ML models use security features from the most recent year available (2024)")
    w("and FILOSOFI features from 2021, applied retrospectively to 2012/2017/2022")
    w("elections. This constitutes temporal leakage and is documented as a known")
    w("methodological limitation of this POC.")
    w("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
#  Console printer (for run_quality.py)
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(results: list, summary: dict) -> None:
    """Print a concise summary to stdout."""
    SEP = "─" * 65

    print()
    print("╔" + "═" * 63 + "╗")
    print("║  ELECTIO ANALYTICS — DATA QUALITY REPORT" + " " * 21 + "║")
    print("╚" + "═" * 63 + "╝")
    print()
    print(f"  Total checks  : {summary['total']}")
    print(f"  ✅ Passed     : {summary['passed']}")
    print(f"  ❌ Failed     : {summary['failed']}")
    print(f"  ⚠️  Warned    : {summary['warned']}")
    print(f"  ⏭️  Skipped   : {summary['skipped']}")
    print(f"  🔴 Critical   : {summary['critical_failures']}")
    print(f"  Health score  : {summary['health_score']:.1f}%")
    print()
    print(SEP)

    # Per-domain
    domain_order = ["election", "security", "filosofi", "cross"]
    for domain in domain_order:
        d = summary["by_domain"].get(domain, {})
        if not d:
            continue
        fails = d.get("failed", 0)
        warns = d.get("warned", 0)
        icon  = "✅" if fails == 0 and warns == 0 else ("❌" if fails > 0 else "⚠️ ")
        print(f"  {icon}  {domain.upper():<12} "
              f"pass={d.get('passed',0):>3}  "
              f"fail={fails:>3}  "
              f"warn={warns:>3}  "
              f"skip={d.get('skipped',0):>3}")
    print(SEP)

    # List failures and warnings
    failures = [r for r in results if r.status == "FAIL"]
    warnings = [r for r in results if r.status == "WARN"]

    if failures:
        print()
        print("  FAILURES:")
        for r in sorted(failures, key=lambda x: SEV_ORDER.get(x.severity, 9)):
            print(f"    ❌ [{r.severity}] {r.name}")
            print(f"       {r.details}")

    if warnings:
        print()
        print("  WARNINGS:")
        for r in warnings:
            print(f"    ⚠️  [{r.severity}] {r.name}")
            print(f"       {r.details}")

    print()
