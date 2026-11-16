"""Small helpers to read configuration from environment variables.

Inside Docker, `docker compose` injects the values from `.env`.
When running a script directly on the laptop we also try to load `.env`
with python-dotenv (if it is installed), so both ways behave the same.
"""
from __future__ import annotations

import os

try:  # Optional: only needed when running outside Docker.
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - dotenv is a convenience, not a requirement
    pass


def get_str(name: str, default: str | None = None) -> str:
    """Return an environment variable as a string.

    Raises a clear error if the variable is missing and no default is given,
    so a misconfigured container fails fast instead of misbehaving later.
    """
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_int(name: str, default: int) -> int:
    """Return an environment variable converted to int."""
    return int(os.getenv(name, str(default)))


def get_float(name: str, default: float) -> float:
    """Return an environment variable converted to float."""
    return float(os.getenv(name, str(default)))


def postgres_dsn() -> str:
    """Build the connection string for the `fleet` business database."""
    return (
        f"postgresql://{get_str('POSTGRES_USER')}:{get_str('POSTGRES_PASSWORD')}"
        f"@{get_str('POSTGRES_HOST', 'postgres')}:{get_str('POSTGRES_PORT', '5432')}"
        f"/{get_str('POSTGRES_DB', 'fleet')}"
    )
