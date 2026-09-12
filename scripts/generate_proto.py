"""Run from the repository root after installing .[dev]."""

from pathlib import Path
from grpc_tools import protoc

root = Path(__file__).resolve().parents[1]
out = root / "src/fraud_pipeline/generated"
result = protoc.main(
    [
        "grpc_tools.protoc",
        f"-I{root / 'contracts'}",
        f"--python_out={out}",
        f"--grpc_python_out={out}",
        str(root / "contracts/fraud.proto"),
    ]
)
if result:
    raise SystemExit(result)
binding = out / "fraud_pb2_grpc.py"
binding.write_text(binding.read_text().replace("import fraud_pb2 as", "from . import fraud_pb2 as"))
