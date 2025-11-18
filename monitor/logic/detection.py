from __future__ import annotations

from typing import Iterable, List

from ..models import Trade


def detect_large_trades(trades: Iterable[Trade], min_notional: float = 100_000.0, top_n: int = 20) -> List[Trade]:
    """Return trades sorted by notional, filtered by threshold."""
    filtered = [t for t in trades if t.notional >= min_notional]
    filtered.sort(key=lambda t: t.notional, reverse=True)
    return filtered[:top_n]

