# Reproduce the local checks

Run from the repository root with Python 3.12. The checks below need no Kafka,
Redis, Docker, real dataset, or trained model. The test suite does bind localhost
ports for real gRPC requests and trains a small model from manufactured data.

## Install and verify

In PowerShell, create a development environment and install the declared pins:

```powershell
python -m venv .venv
if ($LASTEXITCODE -ne 0) { throw 'Environment creation failed' }
$py = Join-Path (Get-Location) '.venv/Scripts/python.exe'
& $py -m pip install -c constraints.txt -e '.[dev]'
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
```

For an existing environment, set `$py` to its Python executable instead. When
sharing an interpreter across checkouts, explicitly select this checkout's source
and verify the printed import location before running checks. Do not reinstall
the shared environment's editable package just to select a different checkout.

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
$env:PYTHONDONTWRITEBYTECODE = '1'
& $py -c "import fraud_pipeline; print(fraud_pipeline.__file__)"
if ($LASTEXITCODE -ne 0) { throw 'Import failed' }
& $py -m pip check
if ($LASTEXITCODE -ne 0) { throw 'Dependency check failed' }
& $py scripts/generate_proto.py --check
if ($LASTEXITCODE -ne 0) { throw 'Protobuf bindings do not match' }
& $py -m ruff check src tests scripts benchmarks --exclude src/fraud_pipeline/generated
if ($LASTEXITCODE -ne 0) { throw 'Lint failed' }
& $py -m pytest -q
if ($LASTEXITCODE -ne 0) { throw 'Tests failed' }
& $py -m fraud_pipeline demo
if ($LASTEXITCODE -ne 0) { throw 'Demo failed' }
git diff --check
if ($LASTEXITCODE -ne 0) { throw 'Diff whitespace check failed' }
```

The explicit exit checks matter in Windows PowerShell: a failed native command
can otherwise be followed by a successful command that obscures the failure.
Record each result, the interpreter version, commit, and import location.
`pip check` validates the installed dependency relationships; it does not prove
that a fresh installation works. `constraints.txt` records Windows-tested pins,
not a complete cross-platform lock. Linux dependency resolution and image builds
remain separate checks.

The demo must report 1,000 processed transactions, 12 behavioral features, and an
explicitly untrained heuristic. Its JSON output is `reports/local-demo.json` and
is ignored by Git. This exercises in-process features and scoring, not external
services or trained-model quality.

## Contract changes

`python scripts/generate_proto.py --check` runs the installed protobuf compiler
in a temporary directory, applies the package-relative gRPC import, and compares
both binding files. It ignores LF/CRLF differences but detects missing files and
content changes. It exits nonzero on drift or compiler failure and leaves the
checked-in bindings untouched. The script resolves paths from its own location,
so it can also be invoked by absolute path from another directory.

After intentionally editing `contracts/fraud.proto`, run:

```powershell
& $py scripts/generate_proto.py
if ($LASTEXITCODE -ne 0) { throw 'Generation failed' }
& $py scripts/generate_proto.py --check
if ($LASTEXITCODE -ne 0) { throw 'Generated bindings do not match' }
& $py -m pytest -q tests/test_proto_generation.py tests/test_serving.py
if ($LASTEXITCODE -ne 0) { throw 'Contract or serving tests failed' }
git diff -- contracts/fraud.proto src/fraud_pipeline/generated
```

Review and commit the contract, generated bindings, and affected implementation
and regression tests together. A compiler-version mismatch should be investigated
against `.[dev]` and `constraints.txt` before accepting generated changes.

## External validation gates

| Gate | Required evidence | What local unit checks do not establish |
| --- | --- | --- |
| Remote CI | Workflow URL, tested commit SHA, conclusion, failing step if any | Local passes do not establish a remote pass |
| Clean setup | Successful install in a new environment and all local checks | Reusing a working environment does not test dependency resolution |
| Container integration | Compose config/build/start results, smoke output with 20 accepted events, parity, model version and demo flag | Docker-free demo does not exercise Kafka or Redis |
| Worker recovery | Completed process-kill trial count, seed and any failed trial diagnostics | Checkpoint simulations are not process-kill trials |
| Model and performance | Dataset/split provenance or workload/topology/duration, actual metrics and errors | Manufactured training and short smoke checks are not quality or capacity evidence |

Inspect the remote run for the exact commit under review; an older green run does
not validate local changes. If credentials, network, Docker, or dependencies are
unavailable, report that gate as unverified and retain the failure reason.

Use the [runbook](RUNBOOK.md) for the full stack and recovery, the
[benchmark guide](BENCHMARKS.md) for measurement, and the
[validation report](../reports/VALIDATION.md) for observed results and limitations.
