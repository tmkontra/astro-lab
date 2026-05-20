"""
TaskFlow DAG that computes a 7-day orders-by-customer report from Postgres.

Seed the source tables with ``etc/orders_report_seed.sql`` first, then trigger
this DAG to refresh ``customer_order_7_day_report``.
"""

from __future__ import annotations

from decimal import Decimal

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task
from pendulum import datetime


DEFAULT_ARGS = {"owner": "project-lab", "retries": 2}
POSTGRES_CONN_ID = "aiven-psql-01"


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


@dag(
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["project-lab", "taskflow", "postgres", "reporting"],
)
def orders_by_customer_report():
    refresh_report_table = SQLExecuteQueryOperator(
        task_id="refresh_report_table",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/refresh_customer_order_7_day_report.sql",
        parameters={"report_date": "{{ ds }}"},
    )

    fetch_report_preview = SQLExecuteQueryOperator(
        task_id="fetch_report_preview",
        conn_id=POSTGRES_CONN_ID,
        sql="sql/orders_report_preview.sql",
        parameters={"report_date": "{{ ds }}"},
    )

    @task
    def publish_report_preview(report_rows: list[tuple[object, ...]]) -> None:
        if not report_rows:
            print("No paid, shipped, or delivered orders found in the past 7 days.")
            return

        columns = [
            "customer_name",
            "customer_tier",
            "order_count",
            "total_revenue",
            "average_order_value",
            "most_recent_order_ts",
        ]

        print("Orders by customer for the past 7 days:")
        for row in report_rows:
            printable_row = {
                column: _json_safe(value) for column, value in zip(columns, row)
            }
            print(printable_row)

    refresh_report_table >> fetch_report_preview
    publish_report_preview(fetch_report_preview.output)


orders_by_customer_report()
