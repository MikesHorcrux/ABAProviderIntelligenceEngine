#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.pipeline import PipelineRunner


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CannaRadar production CLI")
    parser.add_argument("--db", default=str(Path(__file__).resolve().parent / "data" / "cannaradar_v1.db"))
    parser.add_argument("--db-timeout-ms", type=int, default=30000)
    sub = parser.add_subparsers(dest="command", required=True)

    crawl = sub.add_parser("crawl:run", help="Run discovery + fetch + enrich + score + exports")
    crawl.add_argument("--seeds", default="seeds.csv")
    crawl.add_argument("--max", type=int, default=None)
    crawl.add_argument("--export-tier", default="A")
    crawl.add_argument("--export-limit", type=int, default=200)

    enrich = sub.add_parser("enrich:run", help="Re-run enrichment stage (from crawl results)")
    enrich.add_argument("--since", default=None, help='ISO timestamp, e.g. "2026-02-17T00:00:00"')

    score = sub.add_parser("score:run", help="Recompute lead scores")

    export_outreach = sub.add_parser("export:outreach", help="Export outreach-ready CSV")
    export_outreach.add_argument("--tier", default="A")
    export_outreach.add_argument("--limit", type=int, default=200)

    export_research = sub.add_parser("export:research", help="Export research queue")
    export_research.add_argument("--limit", type=int, default=200)

    quality = sub.add_parser("quality:report", help="Write data quality report")
    schema = sub.add_parser("schema:check", help="Validate schema migration metadata")
    return parser


def main() -> None:
    parser = make_parser()
    args = parser.parse_args()

    runner = PipelineRunner(db_path=args.db)
    if args.command == "crawl:run":
        runner.max_pages = args.max
        result = runner.run_crawl(seed_limit=args.max)
        print(f"Crawl run completed: {result}")
        return

    if args.command == "enrich:run":
        updated = runner.run_enrich(since=args.since)
        print(f"Enriched locations: {len(updated)}")
        return

    if args.command == "score:run":
        runner.run_score()
        print("Scoring complete.")
        return

    if args.command == "export:outreach":
        output = runner.run_export(tier=args.tier, limit=args.limit, research_limit=0)
        print(f"Outreach export: {output['outreach']}")
        return

    if args.command == "export:research":
        output = runner.run_export(tier="C", limit=0, research_limit=args.limit)
        print(f"Research queue: {output['research']}")
        return

    if args.command == "quality:report":
        output = runner.run_quality()
        print(f"Quality report: {output['json']}")
        return

    if args.command == "schema:check":
        print("Schema check is handled during ingestion/bootstrap.")
        return

    raise SystemExit(1)


if __name__ == "__main__":
    main()
