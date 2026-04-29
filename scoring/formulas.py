"""
Deterministic scoring formulas for the Verixio Rating Engine.

All inputs are assumed to be normalised to 0–100 before these functions
are called.  The output is always an integer in 0–100.
"""

from __future__ import annotations


def clamp(value: float) -> int:
    """Clamp a float to the integer range [0, 100]."""
    return max(0, min(100, round(value)))


# ── Input scores ──────────────────────────────────────────────────────────────

def permit_score(permits_last_12mo: int, max_citywide_permits: int) -> int:
    """PS = 100 * (permits_last_12mo / max_citywide_permits)"""
    if max_citywide_permits <= 0:
        return 0
    return clamp(100 * permits_last_12mo / max_citywide_permits)


def zoning_score(zoning_favorability: float, recent_zoning_changes: float) -> int:
    """ZS = 100 * (0.7 * zoning_favorability + 0.3 * recent_zoning_changes)

    Both inputs should be 0–1 fractions.
    """
    return clamp(100 * (0.7 * zoning_favorability + 0.3 * recent_zoning_changes))


def friction_score(complaints_30d: int, max_citywide_311: int) -> int:
    """FS = 100 - 100 * (311_30d / max_citywide_311)"""
    if max_citywide_311 <= 0:
        return 100
    return clamp(100 - 100 * complaints_30d / max_citywide_311)


def crime_score(crime_90d: int, max_citywide_crime: int) -> int:
    """CS = 100 - 100 * (crime_90d / max_citywide_crime)"""
    if max_citywide_crime <= 0:
        return 100
    return clamp(100 - 100 * crime_90d / max_citywide_crime)


def environmental_score(env_incidents_12mo: int, max_citywide_env: int) -> int:
    """ES = 100 - 100 * (env_incidents_12mo / max_citywide_env)"""
    if max_citywide_env <= 0:
        return 100
    return clamp(100 - 100 * env_incidents_12mo / max_citywide_env)


# ── Fused scores ──────────────────────────────────────────────────────────────

def nts(ps: int, zs: int, fs: int) -> int:
    """NTS = 0.45*PS + 0.35*ZS + 0.20*FS"""
    return clamp(0.45 * ps + 0.35 * zs + 0.20 * fs)


def tcs(fs: int, cs: int, es: int) -> int:
    """TCS = 0.40*FS + 0.35*CS + 0.25*ES"""
    return clamp(0.40 * fs + 0.35 * cs + 0.25 * es)


def vgd(nts_val: int, tcs_val: int, market_value_percentile: float) -> int:
    """VGD = market_value_percentile - expected_value

    expected_value = 0.55*NTS + 0.45*TCS
    Result is clamped to [-100, 100] and rounded to int.
    """
    expected_value = 0.55 * nts_val + 0.45 * tcs_val
    raw = market_value_percentile - expected_value
    return max(-100, min(100, round(raw)))
