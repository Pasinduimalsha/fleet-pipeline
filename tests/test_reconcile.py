import os

import pandas as pd
import pytest

from airflow.jobs import reconcile


def test_find_pending_day_skips_already_processed(tmp_path):
    for day in (0, 1, 2):
        (tmp_path / f"expenses_day_{day}.csv").write_text("vehicle_id\n")
    assert reconcile.find_pending_day(str(tmp_path), already_processed={0, 1}) == 2
    assert reconcile.find_pending_day(str(tmp_path), already_processed={0, 1, 2}) is None


def test_find_pending_day_picks_earliest(tmp_path):
    for day in (3, 1, 2):
        (tmp_path / f"expenses_day_{day}.csv").write_text("vehicle_id\n")
    assert reconcile.find_pending_day(str(tmp_path), already_processed=set()) == 1


def test_load_expenses_csv_coerces_types(tmp_path):
    path = tmp_path / "expenses_day_0.csv"
    path.write_text(
        "vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag\n"
        "V0001,45.5,0,120.3,False\n"
        "V0002,60.0,150.0,200.0,True\n"
    )
    df = reconcile.load_expenses_csv(str(tmp_path), 0)
    assert df["service_flag"].tolist() == [False, True]
    assert df["fuel_cost"].dtype.kind == "f"


def test_summarize_trips_only_counts_completed():
    events = pd.DataFrame(
        [
            {"vehicle_id": "V0001", "trip_id": "t1", "fare": 10.0, "trip_completed": True},
            {"vehicle_id": "V0001", "trip_id": "t1", "fare": 0.0, "trip_completed": False},
            {"vehicle_id": "V0001", "trip_id": "t2", "fare": 15.0, "trip_completed": True},
            {"vehicle_id": "V0002", "trip_id": "t3", "fare": 8.0, "trip_completed": True},
        ]
    )
    summary = reconcile.summarize_trips(events)
    v1 = summary[summary.vehicle_id == "V0001"].iloc[0]
    assert v1.trips == 2
    assert v1.earnings == 25.0


def test_summarize_trips_handles_empty_input():
    summary = reconcile.summarize_trips(pd.DataFrame(columns=["vehicle_id", "trip_id", "fare", "trip_completed"]))
    assert summary.empty


def test_build_daily_profitability_flags_unprofitable_vehicle():
    trip_summary = pd.DataFrame([{"vehicle_id": "V0001", "trips": 10, "earnings": 100.0}])
    expenses = pd.DataFrame(
        [{"vehicle_id": "V0001", "fuel_cost": 80.0, "maintenance_cost": 40.0, "distance_covered": 100.0, "service_flag": True}]
    )
    result = reconcile.build_daily_profitability(trip_summary, expenses, day=5, unprofitable_threshold=0.0)
    row = result.iloc[0]
    assert row.profit == pytest.approx(100.0 - 80.0 - 40.0)
    assert bool(row.unprofitable) is True
    assert bool(row.has_expenses) is True
    assert row.sim_day == 5


def test_build_daily_profitability_marks_missing_expenses():
    trip_summary = pd.DataFrame([{"vehicle_id": "V0002", "trips": 3, "earnings": 40.0}])
    expenses = pd.DataFrame(columns=["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"])
    result = reconcile.build_daily_profitability(trip_summary, expenses, day=5)
    row = result.iloc[0]
    assert bool(row.has_expenses) is False
    assert pd.isna(row.profit)  # can't judge profitability without cost data
    assert bool(row.unprofitable) is False  # unknown, not flagged


def test_build_daily_profitability_handles_vehicle_with_expenses_but_no_trips():
    trip_summary = pd.DataFrame(columns=["vehicle_id", "trips", "earnings"])
    expenses = pd.DataFrame(
        [{"vehicle_id": "V0003", "fuel_cost": 20.0, "maintenance_cost": 0.0, "distance_covered": 0.0, "service_flag": False}]
    )
    result = reconcile.build_daily_profitability(trip_summary, expenses, day=5)
    row = result.iloc[0]
    assert row.trips == 0
    assert row.earnings == 0.0
    assert bool(row.has_expenses) is True
    assert row.profit == -20.0


def test_write_report_omits_fabricated_delta_when_no_batch_earnings(tmp_path, monkeypatch):
    # write_report reads from the DB via fetch helpers; patch them so this
    # stays a pure-logic test with no live Postgres connection.
    profitability = pd.DataFrame(
        [{"vehicle_id": "V0001", "trips": 0, "earnings": 0.0, "fuel_cost": None, "maintenance_cost": None,
          "distance_covered": None, "profit": None, "profit_per_km": None, "cost_per_km": None,
          "unprofitable": False, "service_flag": False, "has_expenses": False}]
    )
    monkeypatch.setattr(reconcile, "fetch_profitability", lambda conn, day: profitability)
    monkeypatch.setattr(reconcile, "fetch_one", lambda conn, sql, params=(): None)

    path = reconcile.write_report(conn=None, reports_path=str(tmp_path), day=7)
    content = open(path).read()
    assert "Consistency check not yet computed" in content
    assert os.path.exists(path)
