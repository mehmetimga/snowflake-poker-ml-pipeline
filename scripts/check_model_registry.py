"""Verify active deployment identity, artifact immutability, and audit scope."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ops.registry import validate_registry


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry-dir", type=Path, default=Path("models/registry"))
    args = parser.parse_args()
    result = validate_registry(args.registry_dir.resolve())
    active = result["active"]
    print(
        f"[model-registry-check] active={active['model_name']}:{active['run_id']} "
        f"audit_events={result['audit_events']} hashes=passed gates=passed scope=passed"
    )


if __name__ == "__main__":
    main()
