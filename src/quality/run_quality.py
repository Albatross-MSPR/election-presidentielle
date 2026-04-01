#!/usr/bin/env python3
"""
Electio Analytics — Data Quality Runner
========================================
Usage:
    python3 src/quality/run_quality.py
    python3 src/quality/run_quality.py --domain election
    python3 src/quality/run_quality.py --domain security
    python3 src/quality/run_quality.py --domain filosofi
    python3 src/quality/run_quality.py --domain cross
    python3 src/quality/run_quality.py --output /custom/output/dir

Reports are saved to data/quality_reports/ by default.
Exit code 0 = no CRITICAL failures.
Exit code 1 = at least one CRITICAL failure detected.
"""

import argparse
import pathlib
import sys
import time

# ── Resolve project root (works regardless of where the script is called from)
_HERE = pathlib.Path(__file__).resolve().parent
_ROOT = _HERE
while _ROOT != _ROOT.parent and not (_ROOT / "data" / "gold").is_dir():
    _ROOT = _ROOT.parent

if not (_ROOT / "data" / "gold").is_dir():
    print("ERROR: Could not locate project root (data/gold not found).")
    sys.exit(2)

sys.path.insert(0, str(_ROOT / "src"))

from quality.checks import QualityEngine
from quality.report import generate_report, print_summary, _build_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run data quality checks for Electio Analytics.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--domain",
        choices=["election", "security", "filosofi", "cross", "all"],
        default="all",
        help=(
            "Domain to check:\n"
            "  election  — election gold layer\n"
            "  security  — security gold layer\n"
            "  filosofi  — FILOSOFI gold layer\n"
            "  cross     — cross-domain consistency\n"
            "  all       — all domains (default)"
        ),
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=None,
        help="Output directory for reports (default: data/quality_reports/).",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Print results to stdout only; do not write report files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    output_dir = args.output or (_ROOT / "data" / "quality_reports")

    print(f"\n  Project root : {_ROOT}")
    print(f"  Domain       : {args.domain}")
    print(f"  Output dir   : {output_dir}")
    print()

    # ── Run checks ────────────────────────────────────────────────────────
    engine = QualityEngine(_ROOT)

    t0 = time.perf_counter()
    if args.domain == "all":
        results = engine.run_all()
    else:
        results = engine.run_domain(args.domain)
    elapsed = time.perf_counter() - t0

    print(f"  Ran {len(results)} checks in {elapsed:.2f}s")

    # ── Build summary and print ────────────────────────────────────────────
    summary = _build_summary(results)
    print_summary(results, summary)

    # ── Write report files ────────────────────────────────────────────────
    if not args.no_report:
        json_path, md_path = generate_report(results, output_dir)
        print(f"  📄 JSON report : {json_path}")
        print(f"  📝 MD report   : {md_path}")
        print()

    # ── Exit code ─────────────────────────────────────────────────────────
    critical_failures = summary["critical_failures"]
    if critical_failures:
        print(f"  ⛔ {critical_failures} CRITICAL failure(s) detected — exit code 1")
        return 1

    print("  ✅ All critical checks passed — exit code 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
