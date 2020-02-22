from dataclasses import dataclass
from datetime import datetime

from airflow_plus.models import Operator, DAGContext, DAG
from useless_plugin.useless_hook import UselessHook


@dataclass
class MyOperator(Operator):
    task_id: str
    my_conn: UselessHook

    def execute(self, dag_context: DAGContext):
        print(self.task_id)
        print(dag_context.ds)
        print(self.my_conn.conn_id)


op1 = MyOperator(task_id='op1', my_conn='default_useless')
op2 = MyOperator(task_id='op2', my_conn='default_useless')

with DAG(dag_id='test_dag_using_hook', schedule_interval='@once', start_date=datetime.min) as dag:
    dag >> (op1 >> op2)
