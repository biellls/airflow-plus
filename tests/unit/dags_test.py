from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from airflow import settings
from airflow.models import DagBag

from airflow_plus.airflow_testing import mock_airflow_db
from airflow_plus.models import Operator, DAGContext, DAG


def test_create_airflow_dag():
    with mock_airflow_db() as db:
        settings.PLUGINS_FOLDER = str(Path(__file__).parent / 'example_plugins')
        settings.prepare_syspath()
        db.set_connection(
            conn_id='default_useless',
            conn_type='useless',
        )
        dag_bag = DagBag(
            dag_folder=str(Path(__file__).parent/'example_dags'),
            include_examples=False,
        )
        assert 'test_dag' in dag_bag.dag_ids
        assert 'test_dag_using_hook' in dag_bag.dag_ids


@dataclass
class MyOperator(Operator):
    task_id: str

    def execute(self, dag_context: DAGContext):
        print(self.task_id)
        print(dag_context.ds)


def test_dag_creation():
    op1 = MyOperator(task_id='op1')
    op2 = MyOperator(task_id='op2')
    op3 = MyOperator(task_id='op3')

    with DAG(dag_id='test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
        dag >> (op1 >> op2 >> op3)

    assert len(dag.sub_dag.edges) == 2
    assert (op1, op2) in dag.sub_dag.edges
    assert (op2, op3) in dag.sub_dag.edges
