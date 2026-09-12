import hashlib
import json
from pathlib import Path

import numpy as np

from .domain import MODEL_COLUMNS, SCHEMA_VERSION


def matrix(snapshots):
    return np.asarray(
        [[s["amount"], *[s["features"][name] for name in MODEL_COLUMNS[1:]]] for s in snapshots],
        dtype=np.float32,
    )


class RiskModel:
    def __init__(self, directory, allow_demo=False):
        directory = Path(directory)
        self.demo = False
        if not (directory / "metadata.json").exists():
            if not allow_demo:
                raise FileNotFoundError("Train a model first, or explicitly set ALLOW_DEMO_MODEL=1")
            self.demo, self.version = True, "demo-heuristic-v1"
            return
        from xgboost import XGBClassifier

        metadata = json.loads((directory / "metadata.json").read_text())
        if metadata["columns"] != MODEL_COLUMNS or metadata["schema_version"] != SCHEMA_VERSION:
            raise ValueError("Model and serving feature schema are incompatible")
        model_file = directory / "model.ubj"
        if hashlib.sha256(model_file.read_bytes()).hexdigest() != metadata["model_sha256"]:
            raise ValueError("Model checksum mismatch")
        self.model = XGBClassifier(n_jobs=1)
        self.model.load_model(model_file)
        self.model.set_params(n_jobs=1)
        self.version = metadata["model_version"]

    def predict(self, snapshot):
        if self.demo:
            # Deterministic demo only; never described as trained or calibrated.
            return min(0.99, 0.01 + snapshot["amount"] / 5000 + snapshot["features"]["count_1m"] / 100)
        return float(self.model.predict_proba(matrix([snapshot]))[0, 1])


def train(csv_path, output, seed=42):
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score, roc_curve
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from xgboost import XGBClassifier
    from .data import ieee_events
    from .domain import FeatureEngine

    snapshots, labels, timestamps = [], [], []
    engine = FeatureEngine()
    for event, label in ieee_events(csv_path):
        status, snapshot = engine.process(event)
        if status != "accepted":
            raise ValueError(f"Unexpected training event status: {status}")
        snapshots.append(snapshot)
        labels.append(label)
        timestamps.append(event.timestamp)
        engine.expire()
    if len(labels) < 100:
        raise ValueError("Need at least 100 labeled rows")
    x, y = matrix(snapshots), np.asarray(labels)
    # Split at distinct timestamps so equal-time rows cannot cross boundaries.
    unique = np.unique(timestamps)
    if len(unique) < 10:
        raise ValueError("Need at least 10 distinct timestamps")
    time_array = np.asarray(timestamps)
    cut1, cut2 = unique[int(len(unique) * 0.70)], unique[int(len(unique) * 0.85)]
    masks = [time_array < cut1, (time_array >= cut1) & (time_array < cut2), time_array >= cut2]
    for mask in masks:
        if len(np.unique(y[mask])) != 2:
            raise ValueError("Each chronological split must contain both classes")
    train_mask, validation_mask, test_mask = masks
    model = XGBClassifier(
        n_estimators=500,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.85,
        colsample_bytree=0.9,
        tree_method="hist",
        n_jobs=2,
        random_state=seed,
        eval_metric="auc",
        early_stopping_rounds=30,
    )
    model.fit(
        x[train_mask], y[train_mask], eval_set=[(x[validation_mask], y[validation_mask])], verbose=False
    )
    baseline = make_pipeline(
        SimpleImputer(add_indicator=True, keep_empty_features=True),
        StandardScaler(),
        LogisticRegression(max_iter=500, random_state=seed, solver="liblinear"),
    )
    baseline.fit(x[train_mask], y[train_mask])

    def evaluate(probabilities):
        fpr, tpr, _ = roc_curve(y[test_mask], probabilities)
        return {
            "roc_auc": float(roc_auc_score(y[test_mask], probabilities)),
            "pr_auc": float(average_precision_score(y[test_mask], probabilities)),
            "brier_score": float(brier_score_loss(y[test_mask], probabilities)),
            "recall_at_fpr_1pct": float(max(tpr[fpr <= 0.01], default=0)),
        }

    report = {
        "split": "chronological 70/15/15 by distinct timestamps",
        "cutoffs": [float(cut1), float(cut2)],
        "sizes": [int(m.sum()) for m in masks],
        "prevalence": [float(y[m].mean()) for m in masks],
        "xgboost": evaluate(model.predict_proba(x[test_mask])[:, 1]),
        "logistic_baseline": evaluate(baseline.predict_proba(x[test_mask])[:, 1]),
        "best_iteration": int(model.best_iteration),
        "seed": seed,
        "interpretation": "Uncalibrated risk score; IEEE-CIS proxy features, no geographic inputs",
    }
    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)
    model.save_model(out / "model.ubj")
    checksum = hashlib.sha256((out / "model.ubj").read_bytes()).hexdigest()
    with open(csv_path, "rb") as source:
        dataset_hash = hashlib.file_digest(source, "sha256").hexdigest()
    metadata = {
        "model_version": f"xgb-{checksum[:12]}",
        "model_sha256": checksum,
        "dataset_sha256": dataset_hash,
        "schema_version": SCHEMA_VERSION,
        "columns": MODEL_COLUMNS,
        "parameters": model.get_params(),
        "evaluation": report,
    }
    (out / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (out / "evaluation.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
