"""Forwarding script to root smoke.py."""

import asyncio
from pathlib import Path
import sys

# Ensure project root is in sys.path when running script directly
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from smoke import run_smoke_test

if __name__ == "__main__":
    exit_code = asyncio.run(run_smoke_test())
    sys.exit(exit_code)
