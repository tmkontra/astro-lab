"""
An intentionally noisy DAG for exploring Airflow Graph View.

This DAG uses TaskFlow API tasks and a deliberately dense set of dependency
edges. It models an over-the-top launch control room where every station
appears to coordinate with every other station before a final launch gate.
"""

from __future__ import annotations

from airflow.sdk import dag, task, task_group
from pendulum import datetime


DEFAULT_ARGS = {"owner": "project-lab", "retries": 2}


@dag(
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["project-lab", "crazy", "graph-view"],
)
def crazy_graph_view_labyrinth():
    @task
    def checkpoint(station_name: str) -> str:
        print(f"{station_name} has checked in.")
        return station_name

    def node(task_id: str):
        return checkpoint.override(task_id=task_id)(task_id)

    @task_group(group_id="ignition_layer")
    def ignition_layer():
        return node("ignition")

    @task_group(group_id="intake_layer")
    def intake_layer():
        return [node(f"intake_sensor_{idx}") for idx in range(1, 9)]

    @task_group(group_id="normalize_layer")
    def normalize_layer():
        return [node(f"normalize_signal_{idx}") for idx in range(1, 9)]

    @task_group(group_id="enrich_layer")
    def enrich_layer():
        return [node(f"enrich_context_{idx}") for idx in range(1, 9)]

    @task_group(group_id="route_layer")
    def route_layer():
        return [node(f"route_packet_{idx}") for idx in range(1, 9)]

    @task_group(group_id="arbiter_layer")
    def arbiter_layer():
        return [node(f"arbiter_ring_{idx}") for idx in range(1, 5)]

    @task_group(group_id="simulation_layer")
    def simulation_layer():
        return [node(f"simulation_lane_{idx}") for idx in range(1, 9)]

    @task_group(group_id="audit_layer")
    def audit_layer():
        return [node(f"audit_spoke_{idx}") for idx in range(1, 7)]

    @task_group(group_id="publish_layer")
    def publish_layer():
        return [node(f"publish_channel_{idx}") for idx in range(1, 7)]

    @task_group(group_id="finale_layer")
    def finale_layer():
        return node("final_launch_gate"), node("confetti_for_the_graph_view")

    ignition = ignition_layer()
    intake = intake_layer()
    normalize = normalize_layer()
    enrich = enrich_layer()
    route = route_layer()
    arbiters = arbiter_layer()
    simulations = simulation_layer()
    auditors = audit_layer()
    publish = publish_layer()
    final_gate, confetti = finale_layer()

    ignition >> intake

    for idx, source in enumerate(intake):
        source >> normalize[idx]
        source >> normalize[(idx + 1) % len(normalize)]
        source >> normalize[(idx + 3) % len(normalize)]

    for idx, source in enumerate(normalize):
        source >> enrich[idx]
        source >> enrich[(idx + 2) % len(enrich)]
        source >> enrich[(idx + 5) % len(enrich)]

    for idx, source in enumerate(enrich):
        source >> route[idx]
        source >> route[(idx + 1) % len(route)]
        source >> route[(idx + 4) % len(route)]

    for idx, source in enumerate(route):
        source >> arbiters[idx % len(arbiters)]
        source >> arbiters[(idx + 1) % len(arbiters)]
        source >> simulations[idx]
        source >> simulations[(idx + 3) % len(simulations)]

    for idx, arbiter in enumerate(arbiters):
        arbiter >> simulations[idx]
        arbiter >> simulations[idx + len(arbiters)]
        arbiter >> auditors[idx]
        arbiter >> auditors[(idx + 2) % len(auditors)]

    for idx, source in enumerate(simulations):
        source >> auditors[idx % len(auditors)]
        source >> auditors[(idx + 1) % len(auditors)]
        source >> publish[idx % len(publish)]
        source >> publish[(idx + 2) % len(publish)]

    for idx, auditor in enumerate(auditors):
        auditor >> publish[idx % len(publish)]
        auditor >> publish[(idx + 1) % len(publish)]
        auditor >> final_gate

    publish >> final_gate
    final_gate >> confetti


crazy_graph_view_labyrinth()
