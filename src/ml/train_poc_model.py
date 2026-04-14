"""
Machine Learning Model Training Module for French Election Prediction.

This module implements a logistic regression model to predict whether Emmanuel Macron
will win in a given French commune in the 2022 presidential election second round.
The model uses election results from previous elections (2017, 2022 T1) and security
indicators (2021, 2022) as features.

The module supports two experiments:
1. with_2022_t1: Includes 2022 first round data as features
2. without_2022_t1: Uses only 2017 election data and security indicators

Author: Election Analytics Project
"""

from __future__ import annotations

import csv
import io
import json
import math
import random
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
ELECTION_DIR = ROOT_DIR / "data" / "gold" / "election"
SECURITY_DIR = ROOT_DIR / "data" / "gold" / "security"
OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """
    Read CSV file and return list of dictionaries.
    
    Args:
        path: Path to CSV file (semicolon-delimited, UTF-8 encoded)
    
    Returns:
        List of dictionaries where each dictionary represents a row
    """
    content = path.read_text(encoding="utf-8")
    return list(csv.DictReader(io.StringIO(content), delimiter=";"))


def write_csv_rows(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    """
    Write list of dictionaries to CSV file.
    
    Args:
        path: Path where CSV file will be saved
        rows: List of dictionaries to write
        fieldnames: List of column names (header row)
    """
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_id_commune(value: str) -> str:
    """
    Normalize commune ID to standardized format (dept-commune).
    
    Converts IDs like "69-1" to "69-001" with zero-padded department (2 digits)
    and commune (3 digits) codes.
    
    Args:
        value: Commune ID string (e.g., "69-1", "69001")
    
    Returns:
        Normalized ID in format "DD-CCC" where D=dept, C=commune
    """
    parts = str(value).split("-", 1)
    if len(parts) == 2:
        dept = parts[0].strip().zfill(2)
        commune = parts[1].strip().replace(".0", "").zfill(3)
        return f"{dept}-{commune}"
    return str(value).strip()


def slugify(text: str) -> str:
    """
    Convert text to URL-safe slug format for database/file names.
    
    - Converts to lowercase
    - Removes accented characters (é->e, à->a, etc)
    - Replaces spaces and special characters with underscores
    - Collapses multiple underscores to single underscore
    
    Args:
        text: Input text to convert
    
    Returns:
        Slugified text (e.g., "Jean-Marie LE PEN" -> "jean_marie_le_pen")
    """
    text = text.lower().strip()
    for old, new in [
        ("é", "e"),
        ("è", "e"),
        ("ê", "e"),
        ("ë", "e"),
        ("à", "a"),
        ("â", "a"),
        ("ä", "a"),
        ("î", "i"),
        ("ï", "i"),
        ("ô", "o"),
        ("ö", "o"),
        ("ù", "u"),
        ("û", "u"),
        ("ü", "u"),
        ("ç", "c"),
        ("'", "_"),
        ("-", "_"),
        (" ", "_"),
        ("(", "_"),
        (")", "_"),
        ("/", "_"),
    ]:
        text = text.replace(old, new)
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def to_float(value: str, default: float = 0.0) -> float:
    """
    Safely convert string value to float.
    
    Handles None, empty strings, and invalid numeric formats gracefully.
    
    Args:
        value: String value to convert
        default: Default value if conversion fails (default 0.0)
    
    Returns:
        Float value or default if conversion fails
    """
    if value is None:
        return default
    raw = str(value).strip()
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def to_int(value: str, default: int = 0) -> int:
    """
    Safely convert string value to integer.
    
    Handles None, empty strings, and invalid numeric formats gracefully.
    Converts via float first to handle decimal strings like "1.0".
    
    Args:
        value: String value to convert
        default: Default value if conversion fails (default 0)
    
    Returns:
        Integer value or default if conversion fails
    """
    if value is None:
        return default
    raw = str(value).strip()
    if raw == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def sigmoid(x: float) -> float:
    """
    Compute sigmoid activation function.
    
    Converts any input value to probability (0 to 1) using the sigmoid function.
    Clamps x to [-50, 50] to avoid numerical overflow.
    
    Formula: sigmoid(x) = 1 / (1 + e^(-x))
    
    Args:
        x: Input value
    
    Returns:
        Sigmoid output (probability between 0 and 1)
    """
    x = max(-50.0, min(50.0, x))
    return 1.0 / (1.0 + math.exp(-x))


def dot_product(a: list[float], b: list[float]) -> float:
    """
    Calculate dot product of two vectors.
    
    Args:
        a: First vector
        b: Second vector
    
    Returns:
        Dot product (a · b)
    """
    return sum(x * y for x, y in zip(a, b))


def mean(values: list[float]) -> float:
    """
    Calculate mean (average) of values.
    
    Args:
        values: List of numeric values
    
    Returns:
        Mean value, or 0.0 if list is empty
    """
    return sum(values) / len(values) if values else 0.0


def std(values: list[float], avg: float) -> float:
    """
    Calculate standard deviation of values.
    
    Computes population standard deviation given mean value.
    Returns 1.0 if list is empty to avoid division by zero.
    
    Args:
        values: List of numeric values
        avg: Pre-calculated mean/average of values
    
    Returns:
        Standard deviation, or 1.0 if empty
    """
    if not values:
        return 1.0
    variance = sum((v - avg) ** 2 for v in values) / len(values)
    return math.sqrt(variance) or 1.0


def classification_metrics(y_true: list[int], y_pred: list[int]) -> dict[str, float]:
    """
    Calculate comprehensive classification metrics for binary classification.
    
    Computes accuracy, precision, recall, F1-score, specificity, balanced accuracy,
    and confusion matrix values (TP, TN, FP, FN).
    
    Args:
        y_true: True binary labels (0 or 1)
        y_pred: Predicted binary labels (0 or 1)
    
    Returns:
        Dictionary containing:
        - accuracy: (TP+TN)/(TP+TN+FP+FN)
        - balanced_accuracy: (recall+specificity)/2
        - precision: TP/(TP+FP)
        - recall: TP/(TP+FN) - also called sensitivity/true positive rate
        - f1: 2*precision*recall/(precision+recall)
        - tp, tn, fp, fn: confusion matrix values
    """
    total = len(y_true)
    correct = sum(1 for yt, yp in zip(y_true, y_pred) if yt == yp)
    accuracy = correct / total if total else 0.0

    tp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 1)
    tn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 0)
    fp = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 0 and yp == 1)
    fn = sum(1 for yt, yp in zip(y_true, y_pred) if yt == 1 and yp == 0)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    balanced_accuracy = (recall + specificity) / 2.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "accuracy": accuracy,
        "balanced_accuracy": balanced_accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


def stratified_split(rows: list[dict[str, object]], target_key: str, test_size: float = 0.2, seed: int = 42):
    """
    Split dataset into train/test sets maintaining class distribution.
    
    Ensures that both training and testing sets have approximately the same
    proportions of each class (0 and 1) as the original dataset. Useful for
    preventing bias in imbalanced datasets.
    
    Args:
        rows: List of data samples
        target_key: Dictionary key containing the target/label value (0 or 1)
        test_size: Fraction of data to use for testing (default 0.2 = 20%)
        seed: Random seed for reproducibility
    
    Returns:
        Tuple of (train_rows, test_rows)
    """
    random.seed(seed)
    grouped: dict[int, list[dict[str, object]]] = {0: [], 1: []}
    for row in rows:
        grouped[int(row[target_key])].append(row)

    train_rows: list[dict[str, object]] = []
    test_rows: list[dict[str, object]] = []

    for _, group in grouped.items():
        random.shuffle(group)
        split_idx = max(1, int(round(len(group) * (1 - test_size))))
        train_rows.extend(group[:split_idx])
        test_rows.extend(group[split_idx:])

    random.shuffle(train_rows)
    random.shuffle(test_rows)
    return train_rows, test_rows


def build_election_dataset(include_2022_t1: bool = True) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    """
    Build election features dataset for model training.
    
    Loads election results from gold layer (2017 T1/T2 and 2022 T1/T2) and creates
    features for each commune including:
    - Voter participation metrics (registered, abstentions, voters, blanks, nuls, expressed)
    - Candidate vote shares per election
    - Controls target variable: 1 if MACRON wins 2022 T2, 0 if LE PEN wins
    
    The 2022 second round determines the target (who actually won in that commune).
    Earlier elections provide features to predict this outcome.
    
    Args:
        include_2022_t1: If True, includes 2022 first round data as features (for comparison) 
    
    Returns:
        Tuple of:
        - features: Dict mapping commune_id -> Dict of feature_name -> value
        - targets: Dict mapping commune_id -> target_value (0 or 1)
    """
    dim_election = read_csv_rows(ELECTION_DIR / "dim_election.csv")
    dim_candidat = read_csv_rows(ELECTION_DIR / "dim_candidat.csv")
    fact_resultats = read_csv_rows(ELECTION_DIR / "fact_resultats.csv")

    election_map = {
        row["id_election"]: f"{row['annee_election']}_T{row['tour']}" for row in dim_election
    }
    candidate_map = {
        row["id_candidat"]: {"nom": row["nom"], "nuance": row["nuance"]} for row in dim_candidat
    }

    features: dict[str, dict[str, float]] = {}
    target_scores: dict[str, tuple[float, str]] = {}

    for row in fact_resultats:
        id_commune = normalize_id_commune(row["id_commune"])
        election_key = election_map[row["id_election"]]
        candidate = candidate_map[row["id_candidat"]]

        commune_features = features.setdefault(id_commune, {})

        if candidate["nom"] == "MACRON" and election_key == "2022_T2":
            score = to_float(row["voix"])
            best = target_scores.get(id_commune)
            if best is None or score > best[0]:
                target_scores[id_commune] = (score, "MACRON")
        elif candidate["nom"] == "LE PEN" and election_key == "2022_T2":
            score = to_float(row["voix"])
            best = target_scores.get(id_commune)
            if best is None or score > best[0]:
                target_scores[id_commune] = (score, "LE PEN")

        if election_key != "2022_T2":
            if not include_2022_t1 and election_key == "2022_T1":
                continue
            for metric in [
                "inscrits",
                "abstentions",
                "votants",
                "blancs",
                "nuls",
                "exprimes",
                "pct_abs_ins",
                "pct_vot_ins",
                "pct_blancs_ins",
                "pct_nuls_ins",
            ]:
                commune_features[f"{metric}_{election_key.lower()}"] = to_float(row[metric])

            commune_features[
                f"share_{slugify(candidate['nom'])}_{election_key.lower()}"
            ] = to_float(row["pct_voix_exprimes"])

    targets = {
        id_commune: 1 if winner == "MACRON" else 0
        for id_commune, (_, winner) in target_scores.items()
    }
    return features, targets


def build_security_dataset() -> dict[str, dict[str, float]]:
    """
    Build security indicators features dataset.
    
    Loads public security data (crime rates) from 2021-2022 and creates
    features for each commune including:
    - Crime rate indicators (normalized per 1000 inhabitants)
    - INSEE population metrics
    - INSEE housing metrics
    
    These security indicators may correlate with electoral preferences.
    
    Returns:
        Dict mapping commune_id -> Dict of feature_name -> value
    """
    dim_indicateur = read_csv_rows(SECURITY_DIR / "dim_indicateur_securite.csv")
    fact_securite = read_csv_rows(SECURITY_DIR / "fact_securite.csv")

    indicateur_map = {
        row["id_indicateur_securite"]: slugify(row["indicateur"]) for row in dim_indicateur
    }

    security_features: dict[str, dict[str, float]] = {}

    for row in fact_securite:
        year = to_int(row["annee"])
        if year not in (2021, 2022):
            continue

        id_commune = normalize_id_commune(row["id_commune"])
        commune_features = security_features.setdefault(id_commune, {})
        indicateur_slug = indicateur_map[row["id_indicateur_securite"]]

        commune_features[f"sec_rate_{indicateur_slug}_{year}"] = to_float(row["taux_pour_mille"])
        commune_features[f"insee_pop_{year}"] = to_float(row["insee_pop"])
        commune_features[f"insee_log_{year}"] = to_float(row["insee_log"])

    return security_features


def assemble_dataset(include_2022_t1: bool = True) -> tuple[list[dict[str, object]], list[str]]:
    """
    Combine election and security features into unified dataset.
    
    Merges election features, security features, and target variable into
    a single dataset where each row represents a commune with all available features.
    Handles missing features by filling with None.
    
    Args:
        include_2022_t1: If True, includes 2022 first round election features
    
    Returns:
        Tuple of:
        - dataset_rows: List of dicts with commune data and all features
        - feature_names: Sorted list of all feature names (column headers)
    """
    election_features, targets = build_election_dataset(include_2022_t1=include_2022_t1)
    security_features = build_security_dataset()

    all_feature_names: set[str] = set()
    dataset_rows: list[dict[str, object]] = []

    for id_commune, target in targets.items():
        row: dict[str, object] = {"id_commune": id_commune, "target_macron_wins_2022_t2": target}
        row.update(election_features.get(id_commune, {}))
        row.update(security_features.get(id_commune, {}))
        dataset_rows.append(row)
        all_feature_names.update(k for k in row.keys() if k not in {"id_commune", "target_macron_wins_2022_t2"})

    feature_names = sorted(all_feature_names)
    for row in dataset_rows:
        for feature in feature_names:
            row.setdefault(feature, None)

    return dataset_rows, feature_names


def prepare_matrices(rows: list[dict[str, object]], feature_names: list[str]):
    """
    Convert dataset rows into numeric matrices for model input.
    
    Transforms dictionary-based rows into 2D list of floats for mathematical operations.
    Missing values are converted to NaN for imputation in later steps.
    
    Args:
        rows: List of data dictionaries
        feature_names: List of feature column names to extract
    
    Returns:
        2D list (matrix) where each row is a commune and each column is a feature
    """
    matrix = []
    for row in rows:
        matrix.append([to_float(row[feature], default=float("nan")) for feature in feature_names])
    return matrix


def impute_and_scale(
    train_matrix: list[list[float]],
    test_matrix: list[list[float]],
) -> tuple[list[list[float]], list[list[float]], list[float], list[float]]:
    """
    Impute missing values and standardize features using training data statistics.
    
    Preprocessing steps:
    1. Calculate mean and std dev for each feature using ONLY training data
    2. Replace NaN values with feature mean
    3. Standardize all values: (value - mean) / std_dev
    
    This ensures test data uses training statistics (prevents data leakage).
    
    Args:
        train_matrix: Training data matrix
        test_matrix: Testing data matrix
    
    Returns:
        Tuple of:
        - transformed_train_matrix: Imputed and standardized training data
        - transformed_test_matrix: Imputed and standardized test data
        - means: Mean values used (for reference)
        - stds: Standard deviation values used (for reference)
    """
    n_features = len(train_matrix[0])
    means = []
    stds = []

    for j in range(n_features):
        values = [row[j] for row in train_matrix if not math.isnan(row[j])]
        avg = mean(values)
        s = std(values, avg)
        means.append(avg)
        stds.append(s)

    def transform(matrix: list[list[float]]) -> list[list[float]]:
        transformed = []
        for row in matrix:
            new_row = []
            for j, value in enumerate(row):
                v = means[j] if math.isnan(value) else value
                new_row.append((v - means[j]) / stds[j])
            transformed.append(new_row)
        return transformed

    return transform(train_matrix), transform(test_matrix), means, stds


def fit_logistic_regression(
    X: list[list[float]],
    y: list[int],
    lr: float = 0.05,
    epochs: int = 3000,
    l2: float = 0.001,
) -> tuple[list[float], float]:
    """
    Train logistic regression model using gradient descent with L2 regularization.
    
    Implements binary classification using:
    - Sigmoid activation: converts linear output to probability [0, 1]
    - Binary cross-entropy loss
    - L2 regularization: prevents overfitting by penalizing large weights
    - Gradient descent optimization: iteratively updates weights to minimize loss
    
    Args:
        X: Training feature matrix (n_samples x n_features)
        y: Training target labels (binary 0/1)
        lr: Learning rate - controls step size in gradient descent (default 0.05)
        epochs: Number of training iterations (default 3000)
        l2: L2 regularization coefficient (default 0.001)
    
    Returns:
        Tuple of:
        - weights: Final model weights (one per feature)
        - bias: Final intercept/bias term
    """
    n_features = len(X[0])
    weights = [0.0] * n_features
    bias = 0.0
    n_samples = len(X)

    for _ in range(epochs):
        grad_w = [0.0] * n_features
        grad_b = 0.0

        for row, target in zip(X, y):
            pred = sigmoid(dot_product(row, weights) + bias)
            error = pred - target
            for j in range(n_features):
                grad_w[j] += error * row[j]
            grad_b += error

        for j in range(n_features):
            grad_w[j] = grad_w[j] / n_samples + l2 * weights[j]
            weights[j] -= lr * grad_w[j]
        bias -= lr * (grad_b / n_samples)

    return weights, bias


def predict_classes(X: list[list[float]], weights: list[float], bias: float):
    """
    Generate class predictions and probabilities using trained model.
    
    Applies the trained logistic regression model to new data:
    - Computes predicted probabilities using sigmoid(X*w + b)
    - Converts probabilities to binary class (threshold = 0.5)
    
    Args:
        X: Feature matrix for prediction
        weights: Trained model weights
        bias: Trained model bias
    
    Returns:
        Tuple of:
        - preds: List of binary predictions (0 or 1)
        - probas: List of predicted probabilities for class 1 (Macron win)
    """
    probas = [sigmoid(dot_product(row, weights) + bias) for row in X]
    preds = [1 if p >= 0.5 else 0 for p in probas]
    return preds, probas


def save_outputs(
    experiment_name: str,
    dataset_rows: list[dict[str, object]],
    feature_names: list[str],
    predictions: list[dict[str, object]],
    weights: list[float],
    baseline_metrics: dict[str, float],
    model_metrics: dict[str, float],
) -> None:
    """
    Save model outputs and results to CSV and JSON files.
    
    Generates three CSV files and one JSON file:
    1. model_dataset.csv: Complete dataset with all features
    2. test_predictions.csv: Model predictions on test set
    3. feature_importance.csv: Feature coefficients (sorted by absolute value)
    4. metrics.json: Model performance metrics and metadata
    
    Args:
        experiment_name: Name of experiment (becomes subdirectory name)
        dataset_rows: Complete dataset with features
        feature_names: List of feature column names
        predictions: Model predictions on test set
        weights: Trained model weights
        baseline_metrics: Metrics from simplistic majority-class baseline
        model_metrics: Metrics from trained model
    """
    experiment_dir = OUTPUT_DIR / experiment_name
    experiment_dir.mkdir(parents=True, exist_ok=True)

    dataset_fieldnames = ["id_commune", "target_macron_wins_2022_t2"] + feature_names
    write_csv_rows(experiment_dir / "model_dataset.csv", dataset_rows, dataset_fieldnames)

    prediction_fieldnames = ["id_commune", "target_macron_wins_2022_t2", "predicted_class", "predicted_proba_macron"]
    write_csv_rows(experiment_dir / "test_predictions.csv", predictions, prediction_fieldnames)

    importance_rows = [
        {
            "feature": feature,
            "coefficient": weight,
            "abs_coefficient": abs(weight),
        }
        for feature, weight in sorted(
            zip(feature_names, weights),
            key=lambda item: abs(item[1]),
            reverse=True,
        )
    ]
    write_csv_rows(
        experiment_dir / "feature_importance.csv",
        importance_rows,
        ["feature", "coefficient", "abs_coefficient"],
    )

    metrics_payload = {
        "target_definition": "1 if MACRON wins the commune in 2022 second round, else 0",
        "baseline_metrics": baseline_metrics,
        "model_metrics": model_metrics,
        "dataset_rows": len(dataset_rows),
        "features_count": len(feature_names),
    }
    (experiment_dir / "metrics.json").write_text(json.dumps(metrics_payload, indent=2), encoding="utf-8")


def run_experiment(experiment_name: str, include_2022_t1: bool) -> None:
    """
    Execute a complete machine learning experiment end-to-end.
    
    Pipeline:
    1. Assemble dataset from election and security data
    2. Stratified split into train (80%) and test (20%) sets
    3. Convert to numeric matrices and impute missing values
    4. Standardize features using training data statistics
    5. Train logistic regression model
    6. Generate predictions on test set
    7. Compare against baseline (majority class predictor)
    8. Save results and metrics
    
    Args:
        experiment_name: Name for this experiment (used in output directory name)
        include_2022_t1: If True, includes 2022 first round features
    """
    dataset_rows, feature_names = assemble_dataset(include_2022_t1=include_2022_t1)
    train_rows, test_rows = stratified_split(dataset_rows, "target_macron_wins_2022_t2", test_size=0.2, seed=42)

    train_matrix = prepare_matrices(train_rows, feature_names)
    test_matrix = prepare_matrices(test_rows, feature_names)
    train_matrix, test_matrix, _, _ = impute_and_scale(train_matrix, test_matrix)

    y_train = [int(row["target_macron_wins_2022_t2"]) for row in train_rows]
    y_test = [int(row["target_macron_wins_2022_t2"]) for row in test_rows]

    weights, bias = fit_logistic_regression(train_matrix, y_train)
    pred_classes, pred_probas = predict_classes(test_matrix, weights, bias)

    majority_class = round(sum(y_train) / len(y_train))
    baseline_pred = [majority_class] * len(y_test)

    baseline_metrics = classification_metrics(y_test, baseline_pred)
    model_metrics = classification_metrics(y_test, pred_classes)

    predictions = []
    for row, pred, proba in zip(test_rows, pred_classes, pred_probas):
        predictions.append(
            {
                "id_commune": row["id_commune"],
                "target_macron_wins_2022_t2": row["target_macron_wins_2022_t2"],
                "predicted_class": pred,
                "predicted_proba_macron": proba,
            }
        )

    save_outputs(
        experiment_name,
        dataset_rows,
        feature_names,
        predictions,
        weights,
        baseline_metrics,
        model_metrics,
    )

    print(f"Experiment: {experiment_name}")
    print("Target: Macron wins the commune in 2022 second round")
    print(f"Include 2022 T1 features: {include_2022_t1}")
    print(f"Dataset rows: {len(dataset_rows)}")
    print(f"Feature count: {len(feature_names)}")
    print("Baseline metrics:")
    print(json.dumps(baseline_metrics, indent=2))
    print("Model metrics:")
    print(json.dumps(model_metrics, indent=2))
    print(f"Outputs saved to: {OUTPUT_DIR / experiment_name}")


def main() -> None:
    """
    Execute both experimental configurations and generate comparison.
    
    Runs two experiments:
    1. with_2022_t1: Uses election data from 2017 + 2022 T1 + 2022 T2 + security data
    2. without_2022_t1: Uses only 2017 election data + 2022 T2 + security data
    
    Compares model performance with and without 2022 T1 features to assess
    their predictive value.
    """
    experiments = [
        ("with_2022_t1", True),
        ("without_2022_t1", False),
    ]
    comparison = {}

    for experiment_name, include_2022_t1 in experiments:
        run_experiment(experiment_name, include_2022_t1)
        metrics_path = OUTPUT_DIR / experiment_name / "metrics.json"
        comparison[experiment_name] = json.loads(metrics_path.read_text(encoding="utf-8"))

    (OUTPUT_DIR / "comparison.json").write_text(json.dumps(comparison, indent=2), encoding="utf-8")
    print(f"Comparison saved to: {OUTPUT_DIR / 'comparison.json'}")


if __name__ == "__main__":
    main()
