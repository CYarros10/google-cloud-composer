ENVIRONMENT_NAME="composer-2-small"
LOCATION="us-central1"
DAG_ID="pause_all_dag"

while true; do
  # we want to see if the dag exists and isn't paused
  DAG_PAUSED=$(gcloud composer environments run $ENVIRONMENT_NAME \
    --location $LOCATION dags list -- --output=json | jq --raw-output '.[] | select(.dag_id == "'$DAG_ID'") | .paused')

  if [[ "$DAG_PAUSED" == "False" ]]; then
    echo "DAG FOUND!"
    break  # Exit the loop if the DAG exists
  else
    echo "Waiting..."
    sleep 15  # Adjust the polling interval as needed
  fi
done

echo "complete"