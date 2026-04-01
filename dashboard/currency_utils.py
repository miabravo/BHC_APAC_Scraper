"""
Currency conversion to USD for quarterly tracking.

Tries ``forex-python`` (CurrencyRates) first, then a public HTTPS JSON API fallback
(ExchangeRate-API compatible ``latest/USD`` shape) without requiring an API key
for the free tier endpoint pattern documented by providers.

Set ``EXCHANGE_RATE_API_KEY`` in the environment to use exchangerate-api.com paid
endpoints if you configure a custom ``EXCHANGE_RATE_API_URL``.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

# Simple in-process throttle to respect rate limits when calling external FX APIs.
_last_fx_call_ts: float = 0.0
_MIN_INTERVAL_SEC: float = 1.0


def _throttle() -> None:
    global _last_fx_call_ts
    now = time.monotonic()
    delta = now - _last_fx_call_ts
    if delta < _MIN_INTERVAL_SEC:
        time.sleep(_MIN_INTERVAL_SEC - delta)
    _last_fx_call_ts = time.monotonic()


def convert_to_usd(
    amount: float,
    from_currency: str,
) -> float:
    """
    Convert ``amount`` from ``from_currency`` (ISO 4217, e.g. JPY, AUD, CNY) to USD.

    Raises ValueError on unsupported currency or failed fetch.
    """
    code = from_currency.strip().upper()
    if code == "USD":
        return float(amount)

    _throttle()

    # 1) forex-python
    try:
        from forex_python.converter import CurrencyRates  # type: ignore

        cr = CurrencyRates()
        rate = cr.get_rate(code, "USD")
        return float(amount) * float(rate)
    except Exception:
        pass

    # 2) HTTPS fallback: exchangerate.host (no key) — USD as base, invert
    url = os.environ.get(
        "EXCHANGE_RATE_FALLBACK_URL",
        "https://api.exchangerate.host/latest?base=USD&symbols=" + code,
    )
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "QiagenMarketResearchDashboard/1.0"},
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload: Any = json.loads(resp.read().decode("utf-8"))
        rates = payload.get("rates") or {}
        if code not in rates:
            raise ValueError(f"No rate for {code} in response")
        # rate is USD->code; invert for code->USD
        usd_per_unit = 1.0 / float(rates[code])
        return float(amount) * usd_per_unit
    except (urllib.error.URLError, ValueError, json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Currency conversion failed for {code}: {e}") from e


def format_usd(amount: float) -> str:
    """Human-readable USD for Excel / logs."""
    return f"{amount:,.2f} USD"
