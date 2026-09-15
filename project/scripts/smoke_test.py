"""Forwarding script to root smoke.py."""

import asyncio
import sys
from smoke import run_smoke_test

if __name__ == "__main__":
    exit_code = asyncio.run(run_smoke_test())
    sys.exit(exit_code)
