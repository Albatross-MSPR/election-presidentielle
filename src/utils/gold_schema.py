from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


def resolve_project_root(start: Path | None = None) -> Path:
    root = (start or Path.cwd()).resolve()
    while not (root / "data").exists() and root != root.parent:
        root = root.parent
    return root


def detect_csv_separator(csv_path: Path) -> str:
    sample = csv_path.read_text(encoding="utf-8", errors="ignore")[:4096]
    if not sample.strip():
        return ";"

    try:
        return csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
    except csv.Error:
        return ";"


def read_csv_with_fallback(csv_path: Path, separator: str) -> pd.DataFrame:
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(csv_path, sep=separator, encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            continue

    return pd.read_csv(csv_path, sep=separator, low_memory=False)


def print_gold_csv_headers_and_dtypes(gold_dir: Path | str | None = None) -> dict[str, dict[str, str]]:
    root = resolve_project_root()
    target_dir = Path(gold_dir).expanduser().resolve() if gold_dir else root / "data" / "gold"

    if not target_dir.exists():
        raise FileNotFoundError(f"Gold directory not found: {target_dir}")

    schemas: dict[str, dict[str, str]] = {}

    for csv_path in sorted(target_dir.rglob("*.csv")):
        separator = detect_csv_separator(csv_path)
        relative_path = csv_path.relative_to(root) if csv_path.is_relative_to(root) else csv_path

        try:
            df = read_csv_with_fallback(csv_path, separator)
            schema = {column: str(dtype) for column, dtype in df.dtypes.items()}
        except pd.errors.EmptyDataError:
            schema = {}

        schemas[str(relative_path)] = schema

        print(f"\n{relative_path}")
        if not schema:
            print("  <empty file>")
            continue

        for column_name, dtype in schema.items():
            print(f"  {column_name}: {dtype}")

    if not schemas:
        print(f"No CSV files found under {target_dir}")

    return schemas


if __name__ == "__main__":
    print_gold_csv_headers_and_dtypes()
