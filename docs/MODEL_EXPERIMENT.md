# Validation-only class-weight experiment

Protocol recorded 2026-09-15, before candidate training or validation comparison.
Implementation inspected at `9914e6a`. This document specifies proposed training
work; the existing `train` command has **not** yet gained a validation-only mode.

## Hypothesis and bounded change

The current unweighted XGBoost objective may underemphasize rare fraud examples.
Compare it with exactly one candidate using
`scale_pos_weight = training_negative_count / training_positive_count`.
Use counts from the training partition only: the audited input implies
`399858 / 14588` (approximately 27.410). This is a hypothesis, not a promised
improvement. Weighting can degrade ranking and probability calibration.

Keep schema version 1, all 13 ordered model columns, adapter, partitions, seed,
and every other training parameter fixed. No feature search, identity-table join,
resampling, calibration, threshold tuning, or weight grid belongs to this comparison.
The reference is a fresh unweighted XGBoost run on the same input; the historical
logistic result is contextual evidence, not the candidate's comparison arm.

| Parameter | Both arms |
| --- | --- |
| `n_estimators`, `max_depth`, `learning_rate` | 500, 5, 0.05 |
| `subsample`, `colsample_bytree` | 0.85, 0.9 |
| `tree_method`, `n_jobs`, `random_state` | `hist`, 2, 42 |
| `eval_metric`, `early_stopping_rounds` | `auc`, 30 |
| `scale_pos_weight` | Reference: explicitly 1; candidate: training ratio |

Retain validation ROC-AUC early stopping to isolate the weighting change.
Report each arm's zero-based best iteration. Compare average precision at those
selected iterations; do not rerun with a different stopping metric after seeing results.

## Available inputs and temporal semantics

| Input or feature group | Availability and restriction |
| --- | --- |
| Current `amount` | `TransactionAmt`, known at scoring; dataset units, no inferred currency conversion |
| Entity key | Hash of `card1`, `card2`, `card3`, `card5`, `addr1`; current-row proxy, not a verified account identity |
| Counts and spend at 1m/5m/1h, mean at 5m, max at 1h | Past events only, windows `[t-window, t)` |
| Seconds since previous | Strictly earlier event retained in the one-hour history; missing without one |
| Merchant count | Adapter provides no merchants; always zero on this dataset |
| Distance and speed | Adapter provides no coordinates; missing, not fabricated zeros |
| Label | `isFraud`, supervised target only; never an event field or model input |
| Other source columns / identity table | Not read into this model |

`MODEL_COLUMNS` in `src/fraud_pipeline/domain.py` is authoritative for order.
Missing historical values become NaN in the float32 matrix. Keep native XGBoost
missing-value handling unchanged. The adapter sorts by `(TransactionDT,
TransactionID)`; equal-time events cannot contribute to one another's history.
Current amount is an input but is excluded from historical aggregates.

Replay chronologically with one feature engine, expiring state after each event.
Carry **unlabeled** past transaction history across training/validation boundaries
as the online worker would. Do not reset at the boundary or populate history with
future events. Earlier validation events may inform later validation features,
but their labels may not. Entity overlap across partitions is expected for this
temporal deployment question; this is not an unseen-account generalization test.
Proxy collisions and missing proxy components remain limitations.

## Frozen chronological partitions and data provenance

Input: external `train_transaction.csv`, read-only. SHA-256:
`3a5c83ab6b3cc13dcabe5ffa9f522307fd5f7f7b6e6f6a60c32284ca6283d642`.

Use sorted unique timestamps `u`, with `c1 = u[int(len(u)*0.70)]` and
`c2 = u[int(len(u)*0.85)]`. These are timestamp fractions, not exact row fractions.

| Partition | Mask | Rows | Positives | Observed timestamp range |
| --- | --- | ---: | ---: | --- |
| Train | `t < 10452989` | 414,446 | 14,588 | 86400–10452942 |
| Validation | `10452989 <= t < 13179648` | 88,013 | 3,010 | 10452989–13179647 |
| Previously inspected test | `t >= 13179648` | 88,081 | 3,065 | 13179648–15811131 |

Fresh read-only audit found 590,540 rows, 573,349 distinct timestamps, no duplicate
TransactionIDs, no nulls in ID/time/label, and labels exactly `{0, 1}`. This was
a structural audit, not a new fit or quality evaluation. Recheck the checksum
and partition assertions before running; abort on mismatch rather than silently
using a new dataset or moving boundaries. Validate finite, nonnegative amounts
through the existing transaction contract during replay.

The [historical baseline](../reports/IEEE_BASELINE.md) already exposed test
ROC-AUC 0.672305 and other test metrics. That exposure influenced awareness of
model limitations; this protocol cannot restore test independence. The supplied
`test_transaction.csv` header has no `isFraud`, so it provides no local labeled
untouched holdout. Do not describe validation as final generalization evidence,
or reuse the exposed test to select the weight. No final holdout evaluation is
planned unless a separately sourced, demonstrably uninspected labeled period
becomes available and its provenance is documented before evaluation.

## Metric and decision fixed before comparison

Primary selection metric: **validation average precision**, calculated by
`sklearn.metrics.average_precision_score` (called `pr_auc` in existing reports;
not trapezoidal area). Report reference, candidate, and candidate-minus-reference.
Select the candidate for further investigation only if this difference is strictly
positive on full-precision values and all leakage/schema checks pass. A tie or
decline retains the reference. Neither outcome authorizes a serving replacement.

Also report validation ROC-AUC, Brier score, and recall at FPR <= 1% using the
existing ROC-curve convention. These are diagnostics, not alternate selection
criteria if average precision loses. Recall summarizes the validation curve;
it does not establish an operational threshold. Report calibration deterioration
explicitly. No significance or 0.90-AUC achievement claim follows from one seed
and one reused early-stopping/selection partition.

## Implementation acceptance checks

Before the real comparison, add an explicit validation-only path with tests:

1. Preserve exact distinct-timestamp masks, no overlap, full coverage, and
   equal-time boundary grouping. Reject insufficient rows/timestamps and
   single-class training or validation partitions clearly.
2. Derive the weight only from training labels. Perturb validation labels and
   assert the weight is unchanged; perturb excluded-period labels and assert
   fitted inputs, validation predictions, and selection metrics are unchanged.
3. Assert fitting sees training rows only and early stopping sees validation
   rows only. No test predictions or test metrics in this path. Filter the
   excluded period before feature replay; structural hashing/split audit may
   inspect the source, but the fitting/evaluation path must not use test targets.
4. Run the adapter leakage checks in `tests/test_training_leakage.py`: labels
   and ignored columns cannot affect inputs; appended future events cannot
   affect a prefix; equal-time/current amounts stay out of history; missing
   merchant/geography behavior and matrix width match the serving schema.
5. Save/load each model and verify schema, checksum, and prediction agreement
   on the same validation snapshot. Keep regression tests with implementation.

The current `python -m fraud_pipeline train` always computes test metrics.
**Do not use that command for this comparison until the new path exists.**
Do not report this planned isolation as already implemented or tested by the
adapter checks. Existing synthetic training tests also exercise the old path;
their manufactured test split is not the real held-out dataset.

## Reproduction and reporting requirements

Run from the checkout under test. Use its `src` on `PYTHONPATH`, disable bytecode
when borrowing an external interpreter, and print `fraud_pipeline.__file__`
to verify import origin. Audited environment: Python 3.12.8, NumPy 2.2.4,
pandas 2.2.3, scikit-learn 1.6.1, XGBoost 3.0.0. See
[reproducibility instructions](REPRODUCIBILITY.md) for interpreter selection.

The following read-only audit reproduces the checksum and split counts. Set
`IEEE_TRAIN_CSV` to the external source path; run with the selected interpreter.

```python
import hashlib
import os
import numpy as np
import pandas as pd

source = os.environ["IEEE_TRAIN_CSV"]
with open(source, "rb") as stream:
    print("sha256", hashlib.file_digest(stream, "sha256").hexdigest())
rows = pd.read_csv(source, usecols=["TransactionID", "TransactionDT", "isFraud"])
u = np.unique(rows.TransactionDT)
c1, c2 = u[int(len(u) * .70)], u[int(len(u) * .85)]
print("rows", len(rows), "distinct timestamps", len(u), "cutoffs", c1, c2)
print("duplicate IDs", rows.TransactionID.duplicated().sum())
print("nulls", rows.isna().sum().sum(), "labels", sorted(rows.isFraud.unique()))
for mask in (rows.TransactionDT < c1,
             (rows.TransactionDT >= c1) & (rows.TransactionDT < c2),
             rows.TransactionDT >= c2):
    split = rows.loc[mask]
    print(len(split), split.isFraud.sum(), split.TransactionDT.min(), split.TransactionDT.max())
```

The eventual comparison report must include exact executable commands, code
commit, dataset hash, cutoffs/counts, full effective parameters for both arms,
seed, package versions, elapsed time, best iterations, validation metrics and
deltas, selection decision, and model SHA-256 values. Store binaries locally in
ignored `artifacts/` subdirectories without overwriting the baseline. Commit
only aggregate reviewer-facing evidence, never data, environments, or binaries.
If the run fails, preserve its stage and error summary and report missing
results; do not substitute synthetic scores or historical test metrics.

## Morning verification scope

No real-data model was fitted and no candidate metric was observed for this
protocol. Fresh dataset audit and adapter tests establish inputs and feature
behavior. The validation-only runner, weight calculation, isolation regression
tests, paired fits, and final comparison report remain implementation work.

Fresh local checks: `python -m pytest -q --basetemp=.pytest_cache/model-protocol-full`
passed **199 tests** in 10.50 seconds, including three new adapter leakage tests.
`python -m ruff check src tests scripts benchmarks --exclude src/fraud_pipeline/generated`
passed. These use the existing environment with checkout-local imports; they
are not new integration, clean-install, or real-data training results.
