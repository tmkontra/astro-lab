"""
Scheduled serial DAG that demonstrates a simple external API use case.

The workflow uses the shared ``weather_api_default`` connection from
``airflow_settings.yaml`` to read a National Weather Service forecast, reshape
it into a small operations summary, and publish a recommendation.
"""

from __future__ import annotations

from airflow.sdk import BaseHook, dag, task
from pendulum import datetime


DEFAULT_ARGS = {"owner": "project-lab", "retries": 2}
WEATHER_CONN_ID = "weather_api_default"


@dag(
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule="0 12 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["project-lab", "serial", "external-system", "scheduled"],
)
def scheduled_serial_weather_pipeline():
    @task
    def load_weather_connection() -> dict[str, str]:
        conn = BaseHook.get_connection(WEATHER_CONN_ID)
        scheme = conn.schema or "https"
        host = conn.host.rstrip("/")

        return {
            "base_url": f"{scheme}://{host}",
            "endpoint": "/gridpoints/OKX/33,37/forecast",
            "user_agent": "astronomer-project-lab/1.0",
        }

    @task
    def fetch_forecast(connection_details: dict[str, str]) -> dict:
        import requests

        url = f"{connection_details['base_url']}{connection_details['endpoint']}"
        headers = {"User-Agent": connection_details["user_agent"]}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            print(f"Forecast API unavailable; using demo fallback. Error: {exc}")
            return {
                "properties": {
                    "periods": [
                        {
                            "name": "Today",
                            "temperature": 95,
                            "temperatureUnit": "F",
                            "windSpeed": "14 mph",
                            "shortForecast": "Partly Sunny",
                        },
                        {
                            "name": "Tonight",
                            "temperature": 61,
                            "temperatureUnit": "F",
                            "windSpeed": "7 mph",
                            "shortForecast": "Mostly Clear",
                        },
                    ]
                }
            }

    @task
    def build_operations_digest(forecast_payload: dict) -> dict[str, object]:
        periods = forecast_payload["properties"]["periods"][:2]
        high_temperature = max(period["temperature"] for period in periods)
        conditions = ", ".join(
            f"{period['name']}: {period['shortForecast']} "
            f"{period['temperature']}{period['temperatureUnit']}"
            for period in periods
        )

        return {
            "market": "NYC",
            "period_count": len(periods),
            "high_temperature": high_temperature,
            "conditions": conditions,
        }

    @task
    def classify_delivery_risk(digest: dict[str, object]) -> dict[str, object]:
        high_temperature = int(digest["high_temperature"])
        risk = "elevated" if high_temperature >= 85 else "normal"
        recommendation = (
            "Add cold-chain capacity for afternoon routes."
            if risk == "elevated"
            else "Run the standard delivery plan."
        )
        return {**digest, "risk": risk, "recommendation": recommendation}

    @task
    def publish_dispatch_plan(plan: dict[str, object]) -> None:
        print(
            "Dispatch weather plan: "
            f"{plan['market']} risk={plan['risk']} "
            f"conditions={plan['conditions']} "
            f"recommendation={plan['recommendation']}"
        )

    connection_details = load_weather_connection()
    forecast_payload = fetch_forecast(connection_details)
    digest = build_operations_digest(forecast_payload)
    dispatch_plan = classify_delivery_risk(digest)
    publish_dispatch_plan(dispatch_plan)


scheduled_serial_weather_pipeline()
