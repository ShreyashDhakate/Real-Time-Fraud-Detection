# IEEE-CIS behavioral baseline

Run date: 2026-09-12. Source: `../ieee-fraud-detection/train_transaction.csv`.

I trained the first XGBoost model using the current serving-compatible schema: transaction amount and 12 historical behavioral features. I kept the chronological split and parameters defined before this dataset run.

| Split | Rows | Fraud prevalence |
| --- | ---: | ---: |
| Training | 414,446 | 3.5199% |
| Validation | 88,013 | 3.4199% |
| Held-out test | 88,081 | 3.4798% |
| Total | 590,540 | |

The split uses distinct event timestamps at the 70% and 85% boundaries. Its time cutoffs are `10452989` and `13179648` in IEEE-CIS `TransactionDT` units.

| Held-out metric | XGBoost | Logistic baseline |
| --- | ---: | ---: |
| ROC-AUC | 0.672305 | 0.571689 |
| PR-AUC (average precision) | 0.064417 | 0.045747 |
| Brier score | 0.035611 | 0.043447 |
| Recall at FPR <=1% | 0.004894 | 0.000000 |

XGBoost selected iteration 300 (zero-based) using validation early stopping. The model is an uncalibrated risk scorer.

**This does not meet the 0.90 ROC-AUC target.** The adapter uses card/address proxies for entities and omits the full anonymized transaction feature set and identity table. Merchant and geographic features are missing for this dataset. This establishes a real baseline, not a strong fraud detector.

Artifacts are saved locally under `artifacts/model/`: `model.ubj`, `metadata.json`, and `evaluation.json`.

Model version: `xgb-06044eadd449`.

Dataset SHA-256: `3a5c83ab6b3cc13dcabe5ffa9f522307fd5f7f7b6e6f6a60c32284ca6283d642`.

Model SHA-256: `06044eadd449a608fcc4c8012d131fc05f16e6113c6f3ee149ccf305a8bcca88`.

The test result is now visible. Future feature/model selection should use validation data and record that this test period has already been inspected. A richer schema would need matching online inputs before replacing the serving model.
