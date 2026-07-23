"""Generate the deterministic 4-6 player multi-table smoke dataset."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pipeline.generator import MultiTableProfile, build_multitable_dataset


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/generator/multitable-smoke-v1.json"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/datasets/multitable-cold-v1"),
    )
    parser.add_argument(
        "--start-at",
        type=_timestamp,
        default=None,
        help="Aware ISO timestamp for the first active simulation window.",
    )
    args = parser.parse_args()

    profile = MultiTableProfile.from_json(args.config)
    manifest = build_multitable_dataset(
        args.output_dir,
        profile,
        start_at=args.start_at,
    )
    counts = {split: values["hands"] for split, values in manifest["splits"].items()}
    print(
        "[multitable] "
        f"wrote={args.output_dir / 'manifest.json'} "
        f"tables={manifest['requested']['tables']} "
        f"seats={manifest['requested']['concurrent_seats']} "
        f"splits={counts}"
    )


if __name__ == "__main__":
    main()
