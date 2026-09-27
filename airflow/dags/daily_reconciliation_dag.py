"""Daily reconciliation DAG -- the Lambda-architecture batch layer.

Runs on a schedule equal to one simulated day (``SIM_DAY_SECONDS``). Each
run: waits for that day's expense file to land, loads it, recomputes
per-vehicle profitability from the raw-event Parquet lake written by the
streaming job, cross-checks totals against the speed layer's windowed
output (Lambda consistency check), and writes the daily markdown report
consumed by the serving API.

Task-to-task hand-off goes through Postgres/the filesystem, not XCom, so
DataFrames never need to be pickled through the metadata DB -- only a
``sim_day`` integer travels via XCom, which also gives the DAG its natural
task ordering.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.sensors.python import PythonSensor

from common.config import settings
from common.db import get_connection, mark_heartbeat
from jobs import reconcile

SIM_DAY_SECONDS = settings.sim_day_seconds


def _pending_day_exists() -> bool:
    with get_connection() as conn:
        processed = reconcile.already_processed_days(conn)
    return reconcile.find_pending_day(settings.landing_path, processed) is not None


@dag(
    dag_id="daily_fleet_reconciliation",
    description="Batch layer: reconcile daily vehicle expenses against speed-layer trip totals.",
    schedule=timedelta(seconds=SIM_DAY_SECONDS),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    tags=["fleet", "batch-layer"],
)
def daily_fleet_reconciliation():

    wait_for_expense_file = PythonSensor(
        task_id="wait_for_expense_file",
        python_callable=_pending_day_exists,
        poke_interval=5,
        timeout=max(60, int(SIM_DAY_SECONDS * 0.9)),
        mode="reschedule",
        soft_fail=True,
    )

    @task
    def determine_pending_day() -> int:
        with get_connection() as conn:
            processed = reconcile.already_processed_days(conn)
        day = reconcile.find_pending_day(settings.landing_path, processed)
        if day is None:
            raise AirflowSkipException("No new expense file landed this cycle.")
        return day

    @task
    def load_expenses(day: int) -> int:
        expenses = reconcile.load_expenses_csv(settings.landing_path, day)
        with get_connection() as conn:
            reconcile.load_expenses_to_db(conn, expenses, day)
        return day

    @task
    def compute_profitability(day: int) -> int:
        expenses = reconcile.load_expenses_csv(settings.landing_path, day)
        events = reconcile.read_raw_events(settings.lake_path, day)
        trip_summary = reconcile.summarize_trips(events)
        profitability = reconcile.build_daily_profitability(
            trip_summary, expenses, day, settings.unprofitable_threshold
        )
        with get_connection() as conn:
            reconcile.save_daily_profitability(conn, profitability)
        return day

    @task
    def run_consistency_check(day: int) -> int:
        with get_connection() as conn:
            reconcile.compute_consistency_check(conn, day)
        return day

    @task
    def generate_report(day: int) -> str:
        with get_connection() as conn:
            return reconcile.write_report(conn, settings.reports_path, day)

    @task
    def record_heartbeat(report_path: str) -> None:
        with get_connection() as conn:
            mark_heartbeat(conn, "batch_reconcile", detail=f"report: {report_path}")

    day = determine_pending_day()
    wait_for_expense_file >> day

    loaded_day = load_expenses(day)
    profitable_day = compute_profitability(loaded_day)
    checked_day = run_consistency_check(profitable_day)
    report_path = generate_report(checked_day)
    record_heartbeat(report_path)


daily_fleet_reconciliation()
