"""
Parallel DAG with four independent tasks.

Each task has no upstream or downstream dependency, so the scheduler can run
all four checks independently in the same DAG run.
"""

from __future__ import annotations

from airflow.sdk import dag, task
from pendulum import datetime


DEFAULT_ARGS = {"owner": "project-lab", "retries": 2}


@dag(
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["project-lab", "parallel", "independent"],
)
def parallel_independent_checks():
    @task
    def check_customer_feed() -> None:
        print("Customer feed schema looks ready.")

    @task
    def check_inventory_feed() -> None:
        print("Inventory feed freshness is within SLA.")

    @task
    def check_pricing_feed() -> None:
        print("Pricing rules loaded successfully.")

    @task
    def check_fulfillment_feed() -> None:
        print("Fulfillment capacity file is present.")

    check_customer_feed()
    check_inventory_feed()
    check_pricing_feed()
    check_fulfillment_feed()


parallel_independent_checks()
