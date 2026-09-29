"""Tests for the daily-batch data source (expense-file generator)."""
import csv
import os
import random
from dataclasses import replace

import sim.expense_generator as expense_generator
from common.config import settings
from common.fleet_model import vehicle_roster


def test_generate_day_file_writes_expected_columns_and_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(expense_generator, "settings", replace(settings, landing_path=str(tmp_path)))
    random.seed(0)
    roster = vehicle_roster(20)

    path = expense_generator.generate_day_file(day=3, roster=roster)

    assert path == str(tmp_path / "expenses_day_3.csv")
    assert os.path.exists(path)
    assert not os.path.exists(path + ".tmp")  # atomic write leaves no temp file behind

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == expense_generator.FIELDNAMES
        rows = list(reader)

    # ~5% of vehicles are deliberately dropped from the feed (real garage/fuel
    # feeds are rarely complete), but for 20 vehicles it should never be all
    # or none of them.
    assert 0 < len(rows) <= len(roster)
    for row in rows:
        assert row["vehicle_id"].startswith("V")
        assert float(row["fuel_cost"]) >= 0.0
        assert float(row["maintenance_cost"]) >= 0.0
        assert float(row["distance_covered"]) > 0.0
        assert row["service_flag"] in ("True", "False")


def test_generate_day_file_names_one_file_per_simulated_day(tmp_path, monkeypatch):
    monkeypatch.setattr(expense_generator, "settings", replace(settings, landing_path=str(tmp_path)))
    roster = vehicle_roster(5)

    path0 = expense_generator.generate_day_file(day=0, roster=roster)
    path1 = expense_generator.generate_day_file(day=1, roster=roster)

    assert os.path.basename(path0) == "expenses_day_0.csv"
    assert os.path.basename(path1) == "expenses_day_1.csv"
    assert os.path.exists(path0) and os.path.exists(path1)
