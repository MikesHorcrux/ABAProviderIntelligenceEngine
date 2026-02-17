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
    crawl.add_argument("--crawl-mode", default="full", choices=["full", "balanced", "growth", "monitor"])
    crawl.add_argument("--discovery-limit", type=int, default=None)
    crawl.add_argument("--monitor-limit", type=int, default=None)
    crawl.add_argument("--stale-days", type=int, default=None)
    crawl.add_argument("--growth-max-pages", type=int, default=None)
    crawl.add_argument("--growth-max-total", type=int, default=None)
    crawl.add_argument("--growth-max-depth", type=int, default=None)
    crawl.add_argument("--monitor-max-pages", type=int, default=None)
    crawl.add_argument("--monitor-max-total", type=int, default=None)
    crawl.add_argument("--monitor-max-depth", type=int, default=None)
    crawl.add_argument("--export-tier", default="A")
    crawl.add_argument("--export-limit", type=int, default=200)
    crawl.add_argument("--weekly-lead-target", type=int, default=None)
    crawl.add_argument("--growth-window-days", type=int, default=None)
    crawl.add_argument("--growth-governor", type=str, default=None, choices=["on", "off"])
    crawl.add_argument("--enforce-fetch-gate", type=str, default=None, choices=["on", "off"])

    enrich = sub.add_parser("enrich:run", help="Re-run enrichment stage (from crawl results)")
    enrich.add_argument("--since", default=None, help='ISO timestamp, e.g. "2026-02-17T00:00:00"')

    score = sub.add_parser("score:run", help="Recompute lead scores")

    export_outreach = sub.add_parser("export:outreach", help="Export outreach-ready CSV")
    export_outreach.add_argument("--tier", default="A")
    export_outreach.add_argument("--limit", type=int, default=200)

    export_research = sub.add_parser("export:research", help="Export research queue")
    export_research.add_argument("--limit", type=int, default=200)

    export_new = sub.add_parser("export:new", help="Export new leads")
    export_new.add_argument("--since", default=None)
    export_new.add_argument("--limit", type=int, default=100)

    export_signals = sub.add_parser("export:signals", help="Export leads with buying signal changes")
    export_signals.add_argument("--since", default=None)
    export_signals.add_argument("--limit", type=int, default=200)

    quality = sub.add_parser("quality:report", help="Write data quality report")
    schema = sub.add_parser("schema:check", help="Validate schema migration metadata")
    return parser


def main() -> None:
    parser = make_parser()
    args = parser.parse_args()

    runner = PipelineRunner(db_path=args.db)
    if args.command == "crawl:run":
        runner.max_pages = args.max
        if args.weekly_lead_target is not None:
            runner.config.weekly_new_lead_target = args.weekly_lead_target
        if args.growth_window_days is not None:
            runner.config.growth_window_days = args.growth_window_days
        if args.growth_governor is not None:
            runner.config.enforce_growth_governor = args.growth_governor == "on"
        if args.enforce_fetch_gate is not None:
            runner.config.require_fetch_success_gate = args.enforce_fetch_gate == "on"
        result = runner.run_crawl(
            seed_limit=args.max,
            crawl_mode=args.crawl_mode,
            discovery_limit=args.discovery_limit,
            monitor_limit=args.monitor_limit,
            stale_days=args.stale_days,
            growth_max_pages=args.growth_max_pages,
            growth_max_total=args.growth_max_total,
            growth_max_depth=args.growth_max_depth,
            monitor_max_pages=args.monitor_max_pages,
            monitor_max_total=args.monitor_max_total,
            monitor_max_depth=args.monitor_max_depth,
        )
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

    if args.command == "export:new":
        output = runner.run_export(since=args.since, limit=0, research_limit=0, new_limit=args.limit, signal_limit=0)
        print(f"New leads: {output['new_leads']}")
        return

    if args.command == "export:signals":
        output = runner.run_export(since=args.since, limit=0, research_limit=0, new_limit=0, signal_limit=args.limit)
        print(f"Buying signal watchlist: {output['buying_signal_watchlist']}")
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
