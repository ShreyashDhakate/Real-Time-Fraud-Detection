"""Exercise the real pinned compiler and verify that drift checks are read-only."""

import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("generate_proto", ROOT / "scripts/generate_proto.py")
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


@pytest.fixture
def project(tmp_path):
    shutil.copytree(ROOT / "contracts", tmp_path / "contracts")
    out = tmp_path / "src/fraud_pipeline/generated"
    shutil.copytree(ROOT / "src/fraud_pipeline/generated", out, ignore=shutil.ignore_patterns("__pycache__"))
    return tmp_path, out


def contents(directory):
    return {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}


def test_checked_in_bindings_match_without_rewrite(project):
    root, out = project
    before = contents(out)
    assert generator.check(root) == 0
    assert contents(out) == before


@pytest.mark.parametrize("name", generator.BINDINGS)
@pytest.mark.parametrize("missing", [False, True])
def test_detects_each_missing_or_stale_binding_without_repair(project, capsys, name, missing):
    root, out = project
    target = out / name
    if missing:
        target.unlink()
    else:
        target.write_text("# stale binding\n", encoding="utf-8")
    before = contents(out)
    assert generator.check(root) == 1
    assert name in capsys.readouterr().out
    assert contents(out) == before


def test_check_accepts_windows_line_endings(project):
    root, out = project
    for name in generator.BINDINGS:
        target = out / name
        target.write_bytes(target.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
    before = contents(out)
    assert generator.check(root) == 0
    assert contents(out) == before


def test_contract_change_detected_then_regeneration_repairs_bindings(project):
    root, out = project
    contract = root / "contracts/fraud.proto"
    with contract.open("a", encoding="utf-8") as stream:
        stream.write("\nmessage SetupProbe { string value = 1; }\n")
    before = contents(out)
    assert generator.check(root) == 1
    assert contents(out) == before
    assert generator.generate(root, out) == 0
    assert generator.check(root) == 0
    assert contents(out) != before
    assert "from . import fraud_pb2 as" in (out / "fraud_pb2_grpc.py").read_text(encoding="utf-8")


def test_invalid_contract_returns_failure_without_changing_bindings(project):
    root, out = project
    (root / "contracts/fraud.proto").write_text("not a protobuf contract", encoding="utf-8")
    before = contents(out)
    assert generator.check(root) != 0
    assert contents(out) == before


def test_check_cli_works_from_outside_repository(tmp_path):
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/generate_proto.py"), "--check"],
        cwd=tmp_path, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "bindings match" in result.stdout
