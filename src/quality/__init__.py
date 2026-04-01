"""
Electio Analytics — Data Quality Module
========================================
Run:  python3 src/quality/run_quality.py
      python3 src/quality/run_quality.py --domain election
      python3 src/quality/run_quality.py --domain security
      python3 src/quality/run_quality.py --domain filosofi
      python3 src/quality/run_quality.py --domain cross

Reports are written to data/quality_reports/<timestamp>_quality_report.json
and data/quality_reports/<timestamp>_quality_report.md
"""
from .checks import QualityEngine
from .report import generate_report

__all__ = ["QualityEngine", "generate_report"]
