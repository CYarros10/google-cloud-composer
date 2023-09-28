import os
import time
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle

_APPROVED_TAGS = {"customer_success", "op_analytics", "product"}
_LOAD_SECONDS = 2

def test_no_import_errors(dag_bag):
  assert len(dag_bag.import_errors) == 0, "No Import Failures"

def test_valid_schedule_interval(dag_bag):
  import re
  valid_cron_expressions = re.compile("(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|([\d\*]+(\/|-)\d+)|\d+|\*) ?){5,7})")
  for dag in dag_bag.dags:
     schedule = dag_bag.dags[dag].schedule_interval
     valid = re.match(valid_cron_expressions, schedule)
     error_msg = f"DAG {dag} does not have a valid cron expression"
     assert valid, error_msg

def test_owner_present(dag_bag):
  for dag in dag_bag.dags:
      owner = dag_bag.dags[dag].default_args.get('owner')
      error_msg = 'Owner not set for DAG {id}'.format(id=dag)
      assert owner, error_msg

def test_sla_present(dag_bag):
  for dag in dag_bag.dags:
      sla = dag_bag.dags[dag].default_args.get('sla')
      error_msg = 'SLA not set for DAG {id}'.format(id=dag)
      assert sla, error_msg

def test_sla_less_than_timeout(dag_bag):
  for dag in dag_bag.dags:
      sla = dag_bag.dags[dag].default_args.get('sla')
      dagrun_timeout = dag_bag.dags[dag].dagrun_timeout
      error_msg = 'SLA is greater than timeout for DAG {id}'.format(id=dag)
      assert dagrun_timeout > sla, error_msg

def test_retries_present(dag_bag):
  for dag in dag_bag.dags:
      retries = dag_bag.dags[dag].default_args.get('retries', [])
      error_msg = 'Retries not set to 1-4 for DAG {id}'.format(id=dag)
      assert retries > 0 and retries < 5, error_msg

def test_catchup_false(dag_bag):
  for dag in dag_bag.dags:
      catchup = dag_bag.dags[dag].catchup
      error_msg = 'Catchup not set to False for DAG {id}'.format(id=dag)
      assert catchup, error_msg

def test_dag_timeout_set(dag_bag):
  for dag in dag_bag.dags:
      dagrun_timeout = dag_bag.dags[dag].dagrun_timeout
      error_msg = 'DAG Run Timeout not set for DAG {id}'.format(id=dag)
      assert dagrun_timeout, error_msg

def test_dag_description_set(dag_bag):
  for dag in dag_bag.dags:
      description = dag_bag.dags[dag].description
      print(description)
      error_msg = 'Description not set for DAG {id}'.format(id=dag)
      assert description, error_msg

def test_dag_paused_true(dag_bag):
  for dag in dag_bag.dags:
      paused = dag_bag.dags[dag].is_paused_upon_creation
      error_msg = 'Paused not set to True for DAG {id}'.format(id=dag)
      assert paused, error_msg

def test_dag_tags(dag_bag):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    for dag in dag_bag.dags:
        tags = dag_bag.dags[dag].tags
        error_msg = f"{dag} has no tags"
        assert len(tags)>0, error_msg
    if _APPROVED_TAGS:
        assert not set(tags) - _APPROVED_TAGS

def test_dag_task_cycle(dag_bag):
    no_dag_found = True
    for dag in dag_bag.dags:
      no_dag_found = False
      check_cycle(dag_bag.dags[dag])  # Throws if a task cycle is found.

    if no_dag_found:
        raise AssertionError("module does not contain a valid DAG")

def test_import_time(dag_bag):
    """Test that all DAGs can be parsed under the threshold time."""
    path = dag_bag.dag_folder
    dir_list = os.listdir(path)
    total = 0

    for dag_file in dir_list:
      start = time.time()
      dag_bag.process_file(dag_file)
      end = time.time()
      total += end - start

    error_msg = f"DAG file {dag_file} not parsed under threshold time."
    assert total < _LOAD_SECONDS, error_msg

if __name__ == '__main__':
#    with warnings.catch_warnings():
#        warnings.simplefilter('ignore', category=ImportWarning)
    dag_bag = DagBag(dag_folder='dags/', include_examples=False)
    test_no_import_errors(dag_bag)
    test_retries_present(dag_bag)
    test_dag_tags(dag_bag)
    test_catchup_false(dag_bag)
    test_dag_timeout_set(dag_bag)
    test_dag_paused_true(dag_bag)
    test_dag_task_cycle(dag_bag)
    test_sla_present(dag_bag)
    test_sla_less_than_timeout(dag_bag)
    test_import_time(dag_bag)
    test_valid_schedule_interval(dag_bag)