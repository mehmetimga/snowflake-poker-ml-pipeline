#!/usr/bin/env python3
"""Print a release SHA for clean trees and an unmistakable local-dev tag otherwise."""

from __future__ import annotations

try:
    from scripts.check_c1_release import git
except ModuleNotFoundError:  # Direct ``python scripts/c1_image_tag.py`` execution.
    from check_c1_release import git


def default_image_tag() -> str:
    revision = git("rev-parse", "--short=12", "HEAD")
    dirty = git("status", "--porcelain", "--untracked-files=all")
    return f"dev-{revision}" if dirty else revision


if __name__ == "__main__":
    print(default_image_tag())
