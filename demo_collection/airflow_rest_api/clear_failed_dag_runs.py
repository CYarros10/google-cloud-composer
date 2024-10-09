"""
Clear All Failed DAG Runs for retry.
"""

from __future__ import annotations

from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Following GCP best practices, these credentials should be
# constructed at start-up time and used throughout
# https://cloud.google.com/apis/docs/client-libraries-best-practices
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
authed_session = AuthorizedSession(CREDENTIALS)


def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:
    """
    Make a request to Cloud Composer 2 environment's web server.

    Args:
        url: The URL to fetch.
        method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
                'PATCH', 'DELETE')
        **kwargs: Additional parameters for the request function.
                  If no timeout is provided, it is set to 90 by default.
    """
    kwargs.setdefault("timeout", 90)
    response = authed_session.request(method, url, **kwargs)
    response.raise_for_status()
    return response


def clear_dag_run(web_server_url: str, dag_id: str, dag_run_id: str) -> None:
    """
    Clears a DAG run in the Airflow web server.

    Args:
        web_server_url: The URL of the Airflow web server.
        dag_id: The ID of the DAG.
        dag_run_id: The ID of the DAG run.
    """
    endpoint = f"api/v1/dags/{dag_id}/clearTaskInstances"
    request_url = f"{web_server_url}/{endpoint}"

    payload = {
        "dry_run": False,
        "only_failed": False,
        "only_running": False,
        "include_subdags": False,
        "include_parentdag": False,
        "reset_dag_runs": True,
        "dag_run_id": dag_run_id,
        "include_upstream": True,
        "include_downstream": True,
        "include_future": False,
        "include_past": False,
    }

    make_composer2_web_server_request(request_url, method="POST", json=payload)


def clear_failed_dag_runs(
    web_server_url: str, dag_limit: int = 100, dag_run_limit: int = 100
) -> None:
    """
    Clear all failed DAG runs using pagination for both the DAGs and their DAG runs.

    Args:
        web_server_url: The URL of the Airflow 2 web server.
        dag_limit: Number of DAGs to fetch per page (default 100).
        dag_run_limit: Number of DAG runs to fetch per page (default 100).
    """
    cleared_count = 0
    dag_offset = 0

    while True:
        dag_endpoint = f"api/v1/dags?limit={dag_limit}&offset={dag_offset}"
        dag_request_url = f"{web_server_url}/{dag_endpoint}"

        try:
            response = make_composer2_web_server_request(dag_request_url)
            dags = response.json().get("dags", [])
            if not dags:
                break
        except Exception as e:
            logger.error(f"Error fetching DAGs: {e}")
            break

        logger.info(f"Processing {len(dags)} DAG(s)...")

        for dag in dags:
            dag_id = dag["dag_id"]
            dag_run_offset = 0

            while True:
                dag_run_endpoint = (
                    f"api/v1/dags/{dag_id}/dagRuns?limit={dag_run_limit}&offset={dag_run_offset}"
                )
                dag_run_request_url = f"{web_server_url}/{dag_run_endpoint}"

                try:
                    dag_run_response = make_composer2_web_server_request(dag_run_request_url)
                    dag_runs = dag_run_response.json().get("dag_runs", [])
                    if not dag_runs:
                        break
                except Exception as e:
                    logger.error(f"Error fetching DAG runs for DAG '{dag_id}': {e}")
                    break

                for dag_run in dag_runs:
                    if dag_run["state"] == "failed":
                        try:
                            clear_dag_run(
                                web_server_url=web_server_url,
                                dag_id=dag_id,
                                dag_run_id=dag_run["dag_run_id"],
                            )
                            cleared_count += 1
                            logger.info(
                                f"Cleared DAG run '{dag_run['dag_run_id']}' for DAG '{dag_id}'. Total cleared: {cleared_count}"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error clearing DAG run '{dag_run['dag_run_id']}' for DAG '{dag_id}': {e}"
                            )

                if len(dag_runs) < dag_run_limit:
                    break  # No more DAG runs to fetch
                dag_run_offset += dag_run_limit

        if len(dags) < dag_limit:
            break  # No more DAGs to fetch
        dag_offset += dag_limit

    logger.info(f"Total DAG runs cleared: {cleared_count}")

if __name__ == "__main__":

    # Replace web_server_url with the Airflow web server address:
    #     
    # gcloud composer environments describe example-environment \
    #  --location=your-composer-region \
    #  --format="value(config.airflowUri)"
    
    web_server_url = (
        "your-web-server-address"
    )

    clear_failed_dag_runs(web_server_url)