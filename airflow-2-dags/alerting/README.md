# Slack Integration

1. Add library to composer environment.

`apache-airflow-providers-slack`

2. Wait for the Composer environment to finish updating.
   
3. [Create an app on Slack](https://api.slack.com/apps)
- Create app configuration token
- Create app from scratch
- Add features and functionality → incoming webhooks → add new webhook to workspace
- Choose channel
- Copy webhook url
 
4. [Create a connection via Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)

- connection id: your_slack_webhook
- connection type: Slack Incoming Webhook
- description: "your description"
- slack webhook endpoint: hooks.slack.com/services
- webhook token: your-webhook-token-created-in-step-3
- timeout: 30
