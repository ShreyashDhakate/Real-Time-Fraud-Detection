import argparse
import json
import logging
from pathlib import Path
import time

from .config import Settings


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Fraud detection pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("init-topics", "worker", "materialize", "serve"):
        commands.add_parser(name)
    generate = commands.add_parser("generate")
    generate.add_argument("--output", default="data/transactions.jsonl")
    generate.add_argument("--count", type=int, default=1000)
    generate.add_argument("--entities", type=int, default=100)
    generate.add_argument("--seed", type=int, default=42)
    replay = commands.add_parser("replay")
    replay.add_argument("--input", required=True)
    replay.add_argument("--rate", type=float, default=1000)
    batch = commands.add_parser("batch")
    batch.add_argument("--input", required=True)
    batch.add_argument("--output", default="data/features.jsonl")
    backfill = commands.add_parser("backfill")
    backfill.add_argument("--input", required=True)
    backfill.add_argument("--namespace", required=True, help="Separate offline namespace; never the live one")
    training = commands.add_parser("train")
    training.add_argument("--input", required=True, help="IEEE-CIS train_transaction.csv")
    training.add_argument("--output", default="artifacts/model")
    scoring = commands.add_parser("score")
    scoring.add_argument("--target", default="localhost:50051")
    scoring.add_argument("--entity", required=True)
    scoring.add_argument("--transaction", required=True)
    demo = commands.add_parser("demo")
    demo.add_argument("--output", default="reports/local-demo.json")
    args = parser.parse_args()
    settings = Settings()
    if args.command == "init-topics":
        from .streaming import init_topics

        init_topics(settings)
    elif args.command == "worker":
        from .streaming import run_worker

        run_worker(settings)
    elif args.command == "materialize":
        from .store import run_materializer

        run_materializer(settings)
    elif args.command == "serve":
        from .serving import run_server

        run_server(settings)
    elif args.command == "generate":
        from .data import synthetic, write_jsonl

        if args.count < 1 or args.entities < 1:
            parser.error("count and entities must be positive")
        # Keep all generated events in the recent past for immediate scoring.
        write_jsonl(
            args.output,
            (
                x.to_dict()
                for x in synthetic(
                    args.count, args.entities, args.seed, start=time.time() - args.count / 100 - 1
                )
            ),
        )
        print(args.output)
    elif args.command in ("replay", "batch"):
        from .data import batch_rows, read_events, write_jsonl

        if args.command == "replay":
            from .streaming import produce_events

            if args.rate < 0:
                parser.error("rate must be nonnegative (0 means unlimited)")
            print(json.dumps(produce_events(settings, read_events(args.input), args.rate)))
        else:
            write_jsonl(args.output, batch_rows(read_events(args.input)))
    elif args.command == "backfill":
        from .store import FeatureStore

        if args.namespace == settings.namespace:
            parser.error("backfill must use a separate namespace")
        store = FeatureStore(settings.redis_url, args.namespace, settings.feature_ttl)
        count = 0
        with open(args.input, encoding="utf-8") as source:
            for line in source:
                snapshot = json.loads(line)
                snapshot["version"] = "offline:" + snapshot["transaction_id"]
                store.put(snapshot)
                count += 1
        print(json.dumps({"backfilled": count, "namespace": args.namespace}))
    elif args.command == "train":
        from .model import train

        print(json.dumps(train(args.input, args.output), indent=2))
    elif args.command == "score":
        from .serving import score

        print(json.dumps(score(args.target, args.entity, args.transaction), indent=2))
    elif args.command == "demo":
        from .data import batch_rows, synthetic
        from .model import RiskModel

        snapshots = list(batch_rows(synthetic()))
        model = RiskModel("__no_model__", allow_demo=True)
        report = {
            "mode": "in-process; no Kafka/Redis; untrained demo heuristic",
            "processed": len(snapshots),
            "feature_count": len(snapshots[-1]["features"]),
            "example_snapshot": snapshots[-1],
            "example_score": model.predict(snapshots[-1]),
        }
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
