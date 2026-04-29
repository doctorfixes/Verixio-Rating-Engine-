"""Tests for the deterministic scoring formulas."""

import pytest

from scoring.formulas import (
    clamp,
    crime_score,
    environmental_score,
    friction_score,
    nts,
    permit_score,
    tcs,
    vgd,
    zoning_score,
)


# ── clamp ──────────────────────────────────────────────────────────────────────

def test_clamp_mid():
    assert clamp(50.4) == 50


def test_clamp_above_100():
    assert clamp(120) == 100


def test_clamp_below_0():
    assert clamp(-5) == 0


def test_clamp_exact_bounds():
    assert clamp(0) == 0
    assert clamp(100) == 100


# ── permit_score ───────────────────────────────────────────────────────────────

def test_permit_score_max():
    assert permit_score(10, 10) == 100


def test_permit_score_half():
    assert permit_score(5, 10) == 50


def test_permit_score_zero_permits():
    assert permit_score(0, 10) == 0


def test_permit_score_zero_max():
    assert permit_score(5, 0) == 0


# ── zoning_score ───────────────────────────────────────────────────────────────

def test_zoning_score_perfect():
    # 0.7*1 + 0.3*1 = 1.0 → 100
    assert zoning_score(1.0, 1.0) == 100


def test_zoning_score_zero():
    assert zoning_score(0.0, 0.0) == 0


def test_zoning_score_weighted():
    # 0.7*0.8 + 0.3*0.4 = 0.56 + 0.12 = 0.68 → 68
    assert zoning_score(0.8, 0.4) == 68


# ── friction_score ─────────────────────────────────────────────────────────────

def test_friction_score_no_complaints():
    assert friction_score(0, 100) == 100


def test_friction_score_max_complaints():
    assert friction_score(100, 100) == 0


def test_friction_score_half():
    assert friction_score(50, 100) == 50


def test_friction_score_zero_max():
    assert friction_score(5, 0) == 100


# ── crime_score ────────────────────────────────────────────────────────────────

def test_crime_score_no_crime():
    assert crime_score(0, 100) == 100


def test_crime_score_max_crime():
    assert crime_score(100, 100) == 0


def test_crime_score_zero_max():
    assert crime_score(5, 0) == 100


# ── environmental_score ────────────────────────────────────────────────────────

def test_environmental_score_no_incidents():
    assert environmental_score(0, 100) == 100


def test_environmental_score_max_incidents():
    assert environmental_score(100, 100) == 0


def test_environmental_score_zero_max():
    assert environmental_score(3, 0) == 100


# ── nts ────────────────────────────────────────────────────────────────────────

def test_nts_all_100():
    # 0.45*100 + 0.35*100 + 0.20*100 = 100
    assert nts(100, 100, 100) == 100


def test_nts_all_zero():
    assert nts(0, 0, 0) == 0


def test_nts_mixed():
    # 0.45*80 + 0.35*60 + 0.20*40 = 36 + 21 + 8 = 65
    assert nts(80, 60, 40) == 65


# ── tcs ────────────────────────────────────────────────────────────────────────

def test_tcs_all_100():
    assert tcs(100, 100, 100) == 100


def test_tcs_all_zero():
    assert tcs(0, 0, 0) == 0


def test_tcs_mixed():
    # 0.40*70 + 0.35*50 + 0.25*90 = 28 + 17.5 + 22.5 = 68
    assert tcs(70, 50, 90) == 68


# ── vgd ────────────────────────────────────────────────────────────────────────

def test_vgd_balanced():
    # expected = 0.55*50 + 0.45*50 = 50; market=50 → vgd=0
    assert vgd(50, 50, 50) == 0


def test_vgd_undervalued():
    # expected = 0.55*80 + 0.45*80 = 80; market=60 → vgd=-20
    assert vgd(80, 80, 60) == -20


def test_vgd_overvalued():
    # expected = 0.55*40 + 0.45*40 = 40; market=80 → vgd=40
    assert vgd(40, 40, 80) == 40


def test_vgd_clamped_positive():
    # would exceed 100 without clamping
    assert vgd(0, 0, 100) == 100


def test_vgd_clamped_negative():
    assert vgd(100, 100, 0) == -100
