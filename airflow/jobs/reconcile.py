"""Batch layer core logic: daily expense/trip reconciliation.

Every function here is pure or takes an explicit ``conn``, so the logic is
unit-testable without Airflow, Spark or a live database (see
``tests/test_reconcile.py``). ``airflow/dags/daily_reconciliation_dag.py``
wires these into DAG tasks; the DB is used as the hand-off point between
tasks instead of passing large DataFrames through XCom.

Batch layer design note: the daily reconciliation deliberately uses pandas,
not Spark, to recompute the day's numbers from the Parquet lake. One sim
day's trip volume for a class-scale fleet is a few thousand rows -- well
within a single pandas job -- so a Spark cluster would add operational
weight (JVM startup, executor sizing) without a throughput benefit. This is
called out as a "production scale" trade-off in the report.
"""
from __future__ import annotations

import glob
import os
import re
import sys
from datetime import datetime, timezone

import pandas as pd

try:  # option only exists on pandas >= 2.2; harmless to skip on older/newer pandas
    pd.set_option("future.no_silent_downcasting", True)
except pd.errors.OptionError:
    pass

from common.config import settings
from common.db import fetch_all, fetch_one, get_connection, mark_heartbeat, upsert_rows
from common.logging import get_logger

log = get_logger("batch_reconcile")

EXPENSE_FILE_RE = re.compile(r"expenses_day_(\d+)\.csv$")


# ---------------------------------------------------------------- Discovery


def find_pending_day(landing_path: str, already_processed: set[int]) -> int | None:
    """Earliest sim_day with a landed expense file not yet loaded."""
    candidates = []
    for path in glob.glob(os.path.join(landing_path, "expenses_day_*.csv")):
        match = EXPENSE_FILE_RE.search(os.path.basename(path))
        if match and int(match.group(1)) not in already_processed:
            candidates.append(int(match.group(1)))
    return min(candidates) if candidates else None


def already_processed_days(conn) -> set[int]:
    rows = fetch_all(conn, "SELECT DISTINCT sim_day FROM daily_profitability")
    return {r["sim_day"] for r in rows}


# ---------------------------------------------------------------- Expenses


def load_expenses_csv(landing_path: str, day: int) -> pd.DataFrame:
    path = os.path.join(landing_path, f"expenses_day_{day}.csv")
    df = pd.read_csv(path)
    df["service_flag"] = df["service_flag"].astype(str).str.lower().isin(["true", "1", "yes"])
    for col in ("fuel_cost", "maintenance_cost", "distance_covered"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def load_expenses_to_db(conn, expenses: pd.DataFrame, day: int) -> int:
    rows = [
        {
            "vehicle_id": r["vehicle_id"],
            "sim_day": day,
            "fuel_cost": float(r["fuel_cost"]),
            "maintenance_cost": float(r["maintenance_cost"]),
            "distance_covered": float(r["distance_covered"]),
            "service_flag": bool(r["service_flag"]),
        }
        for _, r in expenses.iterrows()
    ]
    return upsert_rows(conn, "expenses", rows, conflict_cols=["vehicle_id", "sim_day"])


# ---------------------------------------------------------------- Trips (raw lake)


def read_raw_events(lake_path: str, day: int) -> pd.DataFrame:
    day_dir = os.path.join(lake_path, "raw_events", f"sim_day={day}")
    files = glob.glob(os.path.join(day_dir, "*.parquet"))
    if not files:
        return pd.DataFrame(columns=["vehicle_id", "trip_id", "fare", "trip_completed"])
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def summarize_trips(events_df: pd.DataFrame) -> pd.DataFrame:
    """Per-vehicle completed-trip count and earnings for one sim day."""
    if events_df.empty:
        return pd.DataFrame(columns=["vehicle_id", "trips", "earnings"])
    completed = events_df[events_df["trip_completed"] == True]  # noqa: E712
    if completed.empty:
        return pd.DataFrame(columns=["vehicle_id", "trips", "earnings"])
    return (
        completed.groupby("vehicle_id")
        .agg(trips=("trip_id", "count"), earnings=("fare", "sum"))
        .reset_index()
    )


# ---------------------------------------------------------------- Join / enrich


def build_daily_profitability(
    trip_summary: pd.DataFrame,
    expenses: pd.DataFrame,
    day: int,
    unprofitable_threshold: float = 0.0,
) -> pd.DataFrame:
    merged = trip_summary.merge(expenses, on="vehicle_id", how="outer", indicator=True)
    merged["trips"] = merged["trips"].fillna(0).astype(int)
    merged["earnings"] = merged["earnings"].fillna(0.0)
    merged["has_expenses"] = merged["_merge"].isin(["both", "right_only"])
    merged["service_flag"] = merged.get("service_flag", pd.Series(dtype=bool)).fillna(False)
    merged = merged.drop(columns=["_merge"])

    total_cost = merged["fuel_cost"].fillna(0.0) + merged["maintenance_cost"].fillna(0.0)
    merged["profit"] = merged["earnings"] - total_cost.where(merged["has_expenses"], other=pd.NA)
    merged["profit"] = pd.to_numeric(merged["profit"], errors="coerce")

    distance = pd.to_numeric(merged.get("distance_covered"), errors="coerce")
    with_distance = distance.where(distance > 0)
    merged["profit_per_km"] = merged["profit"] / with_distance
    merged["cost_per_km"] = total_cost / with_distance

    merged["unprofitable"] = merged["profit"].notna() & (merged["profit"] < unprofitable_threshold)
    merged["sim_day"] = day

    return merged[
        [
            "vehicle_id", "sim_day", "trips", "earnings", "fuel_cost", "maintenance_cost",
            "distance_covered", "profit", "profit_per_km", "cost_per_km", "unprofitable",
            "service_flag", "has_expenses",
        ]
    ]


def save_daily_profitability(conn, profitability: pd.DataFrame) -> int:
    def _num(v):
        return None if pd.isna(v) else float(v)

    rows = [
        {
            "vehicle_id": r["vehicle_id"],
            "sim_day": int(r["sim_day"]),
            "trips": int(r["trips"]),
            "earnings": float(r["earnings"]),
            "fuel_cost": _num(r["fuel_cost"]),
            "maintenance_cost": _num(r["maintenance_cost"]),
            "distance_covered": _num(r["distance_covered"]),
            "profit": _num(r["profit"]),
            "profit_per_km": _num(r["profit_per_km"]),
            "cost_per_km": _num(r["cost_per_km"]),
            "unprofitable": bool(r["unprofitable"]),
            "service_flag": bool(r["service_flag"]),
            "has_expenses": bool(r["has_expenses"]),
        }
        for _, r in profitability.iterrows()
    ]
    return upsert_rows(conn, "daily_profitability", rows, conflict_cols=["vehicle_id", "sim_day"])


def fetch_profitability(conn, day: int) -> pd.DataFrame:
    rows = fetch_all(
        conn,
        "SELECT * FROM daily_profitability WHERE sim_day = %s ORDER BY profit ASC NULLS LAST",
        (day,),
    )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- Lambda consistency check


def compute_consistency_check(conn, day: int) -> dict:
    """Compare the batch layer's recomputed totals against the speed
    layer's windowed totals for the same sim day -- the Lambda-architecture
    sanity check that the two layers agree within tolerance."""
    batch = fetch_one(
        conn,
        "SELECT COALESCE(SUM(trips),0) AS trips, COALESCE(SUM(earnings),0) AS earnings "
        "FROM daily_profitability WHERE sim_day = %s",
        (day,),
    )
    # rt_fleet_metrics holds a 5-minute/1-minute SLIDING window per row, so
    # every event is counted in up to 5 overlapping windows -- summing all
    # rows for the day would inflate the speed-layer total ~5x. Keep only
    # the non-overlapping subset (window_start aligned to a multiple of the
    # window span), which recovers an exact tumbling partition of the day
    # with no double counting. Requires the window span to evenly divide
    # SIM_DAY_SECONDS, true for the shipped 5-minute-window/300s-day default.
    speed = fetch_one(
        conn,
        """
        SELECT COALESCE(SUM(trips_completed), 0) AS trips, COALESCE(SUM(earnings), 0) AS earnings
        FROM rt_fleet_metrics
        WHERE sim_day = %s
          AND MOD(
                EXTRACT(EPOCH FROM window_start)::bigint,
                EXTRACT(EPOCH FROM (window_end - window_start))::bigint
              ) = 0
        """,
        (day,),
    )
    batch_trips, batch_earnings = int(batch["trips"]), float(batch["earnings"])
    speed_trips, speed_earnings = int(speed["trips"]), float(speed["earnings"])

    delta_pct = abs(speed_earnings - batch_earnings) / batch_earnings * 100 if batch_earnings else None

    row = {
        "sim_day": day,
        "speed_trips": speed_trips,
        "batch_trips": batch_trips,
        "speed_earnings": speed_earnings,
        "batch_earnings": batch_earnings,
        "earnings_delta_pct": delta_pct,
    }
    upsert_rows(conn, "consistency_checks", [row], conflict_cols=["sim_day"])
    return row


# ---------------------------------------------------------------- Report


def _fmt(value) -> str:
    return "n/a" if value is None or pd.isna(value) else f"{float(value):.2f}"


def write_report(conn, reports_path: str, day: int) -> str:
    profitability = fetch_profitability(conn, day)
    consistency = fetch_one(conn, "SELECT * FROM consistency_checks WHERE sim_day = %s", (day,))

    os.makedirs(reports_path, exist_ok=True)
    path = os.path.join(reports_path, f"daily_profitability_day_{day}.md")

    total_vehicles = len(profitability)
    unprofitable = profitability[profitability["unprofitable"] == True] if total_vehicles else profitability  # noqa: E712
    top_unprofitable = unprofitable.head(10)

    lines = [
        f"# Daily Fleet Profitability Report -- Sim Day {day}",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Summary",
        f"- Vehicles reporting trips or expenses: {total_vehicles}",
        f"- Unprofitable vehicles: {len(unprofitable)}",
        f"- Total earnings (batch): {profitability['earnings'].sum():.2f}" if total_vehicles else "- Total earnings (batch): 0.00",
        "",
        "## Lambda consistency check (speed layer vs. batch layer)",
    ]
    if consistency:
        delta = consistency["earnings_delta_pct"]
        lines += [
            f"- Speed-layer trips: {consistency['speed_trips']}, earnings: {consistency['speed_earnings']:.2f}",
            f"- Batch-layer trips: {consistency['batch_trips']}, earnings: {consistency['batch_earnings']:.2f}",
            f"- Earnings delta: {delta:.2f}%" if delta is not None else "- Earnings delta: n/a",
        ]
    else:
        lines.append("- Consistency check not yet computed.")

    lines += [
        "",
        "## Top unprofitable vehicles",
        "",
        "| vehicle_id | trips | earnings | fuel_cost | maintenance_cost | profit | cost_per_km | service_flag | has_expenses |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for _, r in top_unprofitable.iterrows():
        lines.append(
            f"| {r['vehicle_id']} | {int(r['trips'])} | {_fmt(r['earnings'])} | "
            f"{_fmt(r['fuel_cost'])} | {_fmt(r['maintenance_cost'])} | {_fmt(r['profit'])} | "
            f"{_fmt(r['cost_per_km'])} | {r['service_flag']} | {r['has_expenses']} |"
        )

    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    os.replace(tmp_path, path)
    return path


# ---------------------------------------------------------------- Orchestration


def run_for_day(day: int) -> dict:
    """End-to-end reconciliation for one sim day in a single connection.

    Used by the manual CLI entry point below and by tests; the Airflow DAG
    calls the smaller functions above as separate tasks instead.
    """
    expenses = load_expenses_csv(settings.landing_path, day)
    events = read_raw_events(settings.lake_path, day)
    trip_summary = summarize_trips(events)
    profitability = build_daily_profitability(trip_summary, expenses, day, settings.unprofitable_threshold)

    with get_connection() as conn:
        load_expenses_to_db(conn, expenses, day)
        save_daily_profitability(conn, profitability)
        consistency = compute_consistency_check(conn, day)
        report_path = write_report(conn, settings.reports_path, day)
        mark_heartbeat(conn, "batch_reconcile", detail=f"day {day}: {len(profitability)} vehicles")

    log.info("day_reconciled", sim_day=day, vehicles=len(profitability), report=report_path)
    return {"sim_day": day, "vehicles": len(profitability), "report_path": report_path, "consistency": consistency}


if __name__ == "__main__":
    target_day = int(sys.argv[1]) if len(sys.argv) > 1 else None
    if target_day is None:
        with get_connection() as conn:
            processed = already_processed_days(conn)
        target_day = find_pending_day(settings.landing_path, processed)
    if target_day is None:
        log.info("no_pending_day")
    else:
        run_for_day(target_day)
