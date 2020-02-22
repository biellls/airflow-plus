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
op3 = MyOperator(task_id='op3')
op4 = MyOperator(task_id='op4')

with DAG(dag_id='test_dag_complex', schedule_interval='@once', start_date=datetime.min) as dag:
    sd = op1 >> op2 >> op3
    dag >> (sd['op2'] >> op4)
