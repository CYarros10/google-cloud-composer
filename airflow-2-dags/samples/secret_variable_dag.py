from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
}


with DAG(
    dag_id='variables_in_bash_dag',
    default_args=default_args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    retrieve_and_echo_variable_via_jinja = BashOperator(
        task_id='retrieve_and_echo_variable_via_jinja',
        bash_command='echo "{{ var.value.my_normal_variable }}"',
    )

    retrieve_and_echo_secret_via_jinja = BashOperator(
        task_id='retrieve_and_echo_secret_via_jinja',
        bash_command='echo "{{ var.value.my_secret }}"',
    )

    retrieve_and_echo_variable_via_jinja
    retrieve_and_echo_secret_via_jinja