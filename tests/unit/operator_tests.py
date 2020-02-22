from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG, settings
from airflow.models import TaskInstance

from airflow_plus.airflow_testing import mock_airflow_db
from airflow_plus.models import Operator, DAGContext


@dataclass
class MyOperator(Operator):
    template_fields = ('sql',)

    task_id: str
    sql: str

    def execute(self, dag_context: DAGContext):
        print(dag_context.ds)
        print(self.sql)


def test_make_airflow_operator(capsys):
    op = MyOperator(task_id='test_op', sql='SELECT 1')
    with mock_airflow_db():
        with DAG(dag_id='test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
            airflow_op = op.airflow_operator
            dag >> airflow_op
            TaskInstance(airflow_op, execution_date=pendulum.datetime(2020, 2, 18)).run(test_mode=True)
            captured = capsys.readouterr()
            assert '2020-02-18' in captured.out
            assert 'SELECT 1' in captured.out


def test_make_airflow_operator_with_render(capsys):
    op = MyOperator(task_id='test_op', sql='SELECT "{{ ds }}"')
    with mock_airflow_db():
        with DAG(dag_id='test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
            airflow_op = op.airflow_operator
            dag >> airflow_op
            TaskInstance(airflow_op, execution_date=pendulum.datetime(2020, 2, 18)).run(test_mode=True)
            captured = capsys.readouterr()
            assert 'SELECT "2020-02-18"' in captured.out


def test_cache_airflow_operator():
    op = MyOperator(task_id='test_op', sql='SELECT 1')
    assert op.airflow_operator is op.airflow_operator


def test_run_airflow_operator(capsys):
    op = MyOperator(task_id='test_op', sql='SELECT 1')
    op.test(execution_date=pendulum.datetime(2019, 2, 19))
    captured = capsys.readouterr()
    assert '2019-02-19' in captured.out
    assert 'SELECT 1' in captured.out


def test_with_hook(capsys):
    with mock_airflow_db() as db:
        settings.PLUGINS_FOLDER = str(Path(__file__).parent / 'example_plugins')
        settings.prepare_syspath()
        db.set_connection(
            conn_id='default_useless',
            conn_type='useless',
        )
        # noinspection PyUnresolvedReferences
        from useless_plugin.useless_hook import UselessHook

        @dataclass
        class MyOperatorWithHook(Operator):
            task_id: str
            my_hook: UselessHook

            def execute(self, dag_context: DAGContext):
                print(self.my_hook.conn_id)

        op = MyOperatorWithHook(task_id='op1', my_hook='default_useless')
        op.test(airflow_db=db)
        captured = capsys.readouterr()
        assert 'default_useless' in captured.out
