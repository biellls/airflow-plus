from dataclasses import dataclass
from datetime import datetime

from airflow_plus.models import Operator, DAGContext, DAG


@dataclass
class MyOperator(Operator):
    task_id: str

    def execute(self, dag_context: DAGContext):
        print(self.task_id)
        print(dag_context.ds)


op1 = MyOperator(task_id='op1')
op2 = MyOperator(task_id='op2')

with DAG(dag_id='test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
    dag >> (op1 >> op2)
