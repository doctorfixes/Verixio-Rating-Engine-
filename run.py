#!/usr/bin/env python
"""
CLI entry-point for running ingestion, scoring, and the change radar.

Usage:
    python run.py ingest           # run all four ingesters
    python run.py ingest permits   # run a single ingester
    python run.py score            # score all parcels
    python run.py radar            # run change radar
    python run.py all              # ingest → score → radar
"""

import argparse
import logging
import sys

from app.database import SessionLocal
from change_radar.radar import run_change_radar
from ingestion.complaints import Complaints311Ingester
from ingestion.crime import CrimeIngester
from ingestion.environmental import EnvironmentalIngester
from ingestion.parcel_seed import ParcelSeeder
from ingestion.permits import PermitsIngester
from scoring.engine import score_all_parcels

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

INGESTERS = {
    "permits": PermitsIngester,
    "complaints": Complaints311Ingester,
    "crime": CrimeIngester,
    "environmental": EnvironmentalIngester,
}


def cmd_seed() -> None:
    db = SessionLocal()
    try:
        n = ParcelSeeder(db).run()
        logger.info("Seeded %d parcels", n)
    finally:
        db.close()


def cmd_ingest(target: str | None = None) -> None:
    db = SessionLocal()
    try:
        targets = {target: INGESTERS[target]} if target else INGESTERS
        for name, cls in targets.items():
            logger.info("Starting ingestion: %s", name)
            count = cls(db).run()
            logger.info("Finished %s: %d records", name, count)
    finally:
        db.close()


def cmd_score() -> None:
    db = SessionLocal()
    try:
        n = score_all_parcels(db)
        logger.info("Scored %d parcels", n)
    finally:
        db.close()


def cmd_radar() -> None:
    db = SessionLocal()
    try:
        n = run_change_radar(db)
        logger.info("Change Radar: %d alerts", n)
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verixio Rating Engine CLI")
    sub = parser.add_subparsers(dest="command")

    ingest_p = sub.add_parser("ingest", help="Run data ingestion")
    ingest_p.add_argument(
        "source",
        nargs="?",
        choices=list(INGESTERS.keys()),
        help="Ingest a specific source (default: all)",
    )

    sub.add_parser("seed", help="Seed parcels from Denver Real Property Valuations")
    sub.add_parser("score", help="Score all parcels")
    sub.add_parser("radar", help="Run Change Radar")
    sub.add_parser("all", help="Ingest → score → radar (run 'seed' first on a fresh DB)")

    args = parser.parse_args()

    if args.command == "seed":
        cmd_seed()
    elif args.command == "ingest":
        cmd_ingest(getattr(args, "source", None))
    elif args.command == "score":
        cmd_score()
    elif args.command == "radar":
        cmd_radar()
    elif args.command == "all":
        cmd_ingest()
        cmd_score()
        cmd_radar()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
