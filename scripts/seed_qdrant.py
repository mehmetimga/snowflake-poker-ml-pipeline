"""Seed Qdrant collusion / normal pattern collections from PAIR_STATS."""

from __future__ import annotations

from pipeline.qdrant.pattern_store import seed_patterns


if __name__ == "__main__":
    seed_patterns()
