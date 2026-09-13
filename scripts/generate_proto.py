"""Generate bindings, or check for drift without modifying the checkout."""

import argparse
from pathlib import Path
import tempfile

from grpc_tools import protoc

BINDINGS = ("fraud_pb2.py", "fraud_pb2_grpc.py")


def generate(root: Path, out: Path) -> int:
    out.mkdir(parents=True, exist_ok=True)
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
        return result
    binding = out / "fraud_pb2_grpc.py"
    binding.write_text(
        binding.read_text(encoding="utf-8").replace("import fraud_pb2 as", "from . import fraud_pb2 as"),
        encoding="utf-8",
        newline="\n",
    )
    return 0


def check(root: Path) -> int:
    checked_in = root / "src/fraud_pipeline/generated"
    with tempfile.TemporaryDirectory(prefix="fraud-proto-") as directory:
        generated = Path(directory)
        result = generate(root, generated)
        if result:
            return result
        drift = [
            name for name in BINDINGS
            if not (checked_in / name).is_file()
            or (checked_in / name).read_text(encoding="utf-8")
            != (generated / name).read_text(encoding="utf-8")
        ]
    if drift:
        print("Protobuf bindings missing or stale: " + ", ".join(drift))
        print("Run python scripts/generate_proto.py with the pinned development dependencies.")
        return 1
    print("Protobuf bindings match contracts/fraud.proto.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check bindings without rewriting them")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.check:
        return check(root)
    return generate(root, root / "src/fraud_pipeline/generated")


if __name__ == "__main__":
    raise SystemExit(main())
