"""
Offline charity catalog helpers for querying the local SQLite database.

This module centralizes all direct access to data/charities.db so the FastAPI
app can fetch charity records by NTEE codes, majors, and states.
"""

from __future__ import annotations

import random
import sqlite3
from pathlib import Path
from typing import List, Optional, TypedDict

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "charities.db"

_CONNECTION: Optional[sqlite3.Connection] = None


class CharityRow(TypedDict):
    ein: str
    name: str
    city: Optional[str]
    state: Optional[str]
    ntee_code: str
    ntee_major: str


def get_connection() -> sqlite3.Connection:
    """
    Return a cached SQLite connection to the charities database.

    The connection is opened in read-only mode (when possible) and re-used
    across calls to avoid re-opening the DB for each query.
    """
    global _CONNECTION
    if _CONNECTION is None:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"Charities database not found at {DB_PATH}")
        uri = f"file:{DB_PATH}?mode=ro"
        _CONNECTION = sqlite3.connect(uri, uri=True, check_same_thread=False)
        _CONNECTION.row_factory = sqlite3.Row
    return _CONNECTION


def _rows_to_charities(rows: List[sqlite3.Row]) -> List[CharityRow]:
    """Convert sqlite rows to CharityRow dicts."""
    results: List[CharityRow] = []
    for row in rows:
        results.append(
            CharityRow(
                ein=row["ein"],
                name=row["name"],
                city=row["city"],
                state=row["state"],
                ntee_code=row["ntee_code"],
                ntee_major=row["ntee_major"],
            )
        )
    return results


def _normalize_deciles(deciles: List[str]) -> List[str]:
    """Return a cleaned list of uppercase decile codes, ignoring empties."""
    cleaned = [d.strip().upper() for d in deciles if isinstance(d, str) and d.strip()]
    return cleaned


def _normalize_state(state: Optional[str]) -> Optional[str]:
    """Normalize state input, treating 'any' (case-insensitive) as None."""
    if not state:
        return None
    state = state.strip().upper()
    if not state or state == "ANY":
        return None
    return state


def _execute_query(query: str, params: List[str]) -> List[sqlite3.Row]:
    """Helper to run a read query and return rows."""
    conn = get_connection()
    cursor = conn.execute(query, params)
    return cursor.fetchall()


def fetch_by_deciles(deciles: List[str], state: Optional[str] = None, limit: int = 100) -> List[CharityRow]:
    """
    Return up to `limit` charities whose ntee_code is in `deciles`.
    If `state` is provided and not equal to 'any', attempt to filter by state first.
    If no rows match with the state filter, fall back to ignoring state.
    """
    cleaned_deciles = _normalize_deciles(deciles)
    if not cleaned_deciles:
        return []

    placeholders = ",".join(["?"] * len(cleaned_deciles))
    base_query = (
        "SELECT ein, name, city, state, ntee_code, ntee_major "
        "FROM charities WHERE ntee_code IN ({placeholders})"
    ).format(placeholders=placeholders)
    order_limit_clause = " ORDER BY RANDOM() LIMIT ?"

    params = cleaned_deciles.copy()
    params.append(limit)

    normalized_state = _normalize_state(state)
    if normalized_state:
        query = f"{base_query} AND state = ?{order_limit_clause}"
        state_params = cleaned_deciles.copy()
        state_params.append(normalized_state)
        state_params.append(limit)
        rows = _execute_query(query, state_params)
        if rows:
            return _rows_to_charities(rows)

    query = base_query + order_limit_clause
    rows = _execute_query(query, params)
    return _rows_to_charities(rows)


def fetch_by_major(major_letter: str, state: Optional[str] = None, limit: int = 100) -> List[CharityRow]:
    """
    Return up to `limit` charities where ntee_major matches the letter.
    Applies the same state filtering fallback as fetch_by_deciles.
    """
    if not major_letter:
        return []
    major_letter = major_letter.strip().upper()
    if not major_letter:
        return []

    base_query = (
        "SELECT ein, name, city, state, ntee_code, ntee_major "
        "FROM charities WHERE ntee_major = ?"
    )
    order_limit_clause = " ORDER BY RANDOM() LIMIT ?"
    normalized_state = _normalize_state(state)

    if normalized_state:
        query = f"{base_query} AND state = ?{order_limit_clause}"
        rows = _execute_query(query, [major_letter, normalized_state, limit])
        if rows:
            return _rows_to_charities(rows)

    query = base_query + order_limit_clause
    rows = _execute_query(query, [major_letter, limit])
    return _rows_to_charities(rows)


def fetch_random_pool_for_deciles(
    deciles: List[str],
    state: Optional[str],
    pool_size: int,
    seed: Optional[int] = None,
) -> List[CharityRow]:
    """
    Fetch a larger pool of charities by deciles (with optional state filter),
    shuffle deterministically if `seed` is provided, and return up to pool_size.
    """
    if pool_size <= 0:
        return []
    large_limit = max(pool_size * 3, pool_size)
    results = fetch_by_deciles(deciles, state=state, limit=large_limit)
    if seed is not None:
        rng = random.Random(seed)
        rng.shuffle(results)
    return results[:pool_size]
