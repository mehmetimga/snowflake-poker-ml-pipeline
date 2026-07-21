#!/usr/bin/env python3
"""Require a clean Git commit before pushing or deploying C1 images."""

from __future__ import annotations

import argparse
import subprocess


def git(*arguments: str) -> str:
    return subprocess.check_output(
        ["git", *arguments], text=True, stderr=subprocess.STDOUT
    ).strip()


def validate_release_tag(tag: str) -> str:
    dirty = git("status", "--porcelain", "--untracked-files=all")
    if dirty:
        raise ValueError(
            "C1 release requires a clean committed worktree; local dev images may "
            "still use a dev-<sha> tag"
        )
    expected = git("rev-parse", "--short=12", "HEAD")
    if tag != expected:
        raise ValueError(f"C1 release tag must equal current Git SHA {expected}, got {tag}")
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()
    try:
        revision = validate_release_tag(args.tag)
    except (ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"[c1-release-check] failed: {error}") from error
    print(f"[c1-release-check] passed revision={revision}")


if __name__ == "__main__":
    main()
