from __future__ import annotations
import datetime
from croniter import croniter

from typing import Any
import json
import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

# Following GCP best practices, these credentials should be
# constructed at start-up time and used throughout
# https://cloud.google.com/apis/docs/client-libraries-best-practices
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])


def generate_time_slot_counts(cron_expressions, start_time, end_time, slot_duration) -> dict:
    """
    Finds available time slots within a given time range based on a list of cron expressions.

    Args:
        cron_expressions (list): A list of cron expressions.
        start_time (datetime.datetime): The start of the search range.
        end_time (datetime.datetime): The end of the search range.
        slot_duration (datetime.timedelta): The desired duration of each available slot.

    Returns:
        list: A list of tuples, where each tuple represents an available time slot 
              (start_datetime, end_datetime).
    """

    execution_times = {}
    current_time = datetime.datetime.now()

    # return a mapping of execution times : # of executions
    for cron_expr in cron_expressions:
        iter = croniter(cron_expr, current_time)  # Iterator for the cron expression
        next_execution = iter.get_next(datetime.datetime)
        while next_execution < end_time:
            if next_execution > start_time:
                execution = next_execution.strftime("%Y-%m-%d %H:%M:%S")
                if execution not in execution_times:
                    execution_times[execution] = 1
                else:
                    execution_times[execution] += 1
            next_execution = iter.get_next(datetime.datetime, next_execution)
    
    print("execution_times:")
    sorted_data = dict(sorted(execution_times.items()))
    print(json.dumps(sorted_data, indent=4))

    interval = start_time
    time_intervals = {}
    while interval <= end_time:

        # Check if the current slot clashes with any cron expression
        for e in execution_times:
            e_dt = datetime.datetime.strptime(e, "%Y-%m-%d %H:%M:%S")
            if interval <= e_dt < interval + slot_duration:
                interval_str = interval.strftime("%Y-%m-%d %H:%M:%S")
                if interval_str not in time_intervals:
                    #print(cron_expr)
                    time_intervals[interval_str] = execution_times[e]
                else:
                    time_intervals[interval_str] += execution_times[e]

        interval += slot_duration

    return time_intervals

def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:
    """
    Make a request to Cloud Composer 2 environment's web server.
    Args:
      url: The URL to fetch.
      method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
        'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                  If no timeout is provided, it is set to 90 by default.
    """

    authed_session = AuthorizedSession(CREDENTIALS)

    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)

def list_dags(web_server_url: str) -> str:
    """
    Make a request to list dags using the stable Airflow 2 REST API.
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

    Args:
      web_server_url: The URL of the Airflow 2 web server.
    """

    endpoint = f"api/v1/dags"
    request_url = f"{web_server_url}/{endpoint}"

    response = make_composer2_web_server_request(
        request_url, method="GET"
    )

    if response.status_code == 403:
        raise requests.HTTPError(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            f"{response.headers} / {response.text}"
        )
    elif response.status_code != 200:
        response.raise_for_status()
    else:
        return response.text

def get_list_of_cron_expressions(web_server_url: str):
    cron_list=[]
    preset_to_cron_mapping = {
            #'@once': None,  # Special case, handled separately
            '@hourly': '0 * * * *',
            '@daily': '0 0 * * *',
            '@weekly': '0 0 * * 0',
            '@monthly': '0 0 1 * *',
            '@yearly': '0 0 1 1 *'
    }

    list_dags_response_text = list_dags(web_server_url)
    dags = json.loads(list_dags_response_text)["dags"]
    for dag in dags:
        if dag['schedule_interval'] != None:
            schedule_interval_val = str(dag['schedule_interval']['value'])
            if schedule_interval_val != "@once":
                if schedule_interval_val in preset_to_cron_mapping:
                    schedule_interval_val = preset_to_cron_mapping[schedule_interval_val]

                cron_list.append(schedule_interval_val)

    print("cron list:")
    print(json.dumps(cron_list, indent=4))
    return cron_list


if __name__ == "__main__":

    # Replace with configuration parameters for the DAG run.
    # Replace web_server_url with the Airflow web server address. To obtain this
    # URL, run the following command for your environment:
    
    # gcloud composer environments describe example-environment \
    #  --location=your-composer-region \
    #  --format="value(config.airflowUri)"

    # gcloud composer environments describe composer-env-small \
    #  --location=us-central1 \
    #  --format="value(config.airflowUri)"
    
    web_server_url = (
        ""
    )
    
    start = datetime.datetime(2024, 4, 9, 0, 0)  # April 9th, 2024 at 0 AM 
    end = datetime.datetime(2024, 4, 9, 0, 59)  # April 9th, 2024 at 6 PM
    duration = datetime.timedelta(minutes=5)   # 10-minute slots


    cron_list = get_list_of_cron_expressions(web_server_url)

    intervals = generate_time_slot_counts(cron_list, start, end, duration)
    sorted_data = dict(sorted(intervals.items(), key=lambda item: item[1]))

    print("time slot availability:")
    print(json.dumps(sorted_data, indent=4))
