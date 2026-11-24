"""Shared pytest setup: import path and a fixed simulated clock."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Fixed anchor so sim-clock results are predictable in every test.
os.environ.setdefault("SIM_ANCHOR_EPOCH", "1800000000")
os.environ.setdefault("SIM_DAY_SECONDS", "600")
os.environ.setdefault("SIM_START_DATE", "2025-01-01")
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
