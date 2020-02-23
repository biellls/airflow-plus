from dataclasses import dataclass

from airflow_plus.models import Operator, DAGContext


@dataclass
class HelloWorldOperator(Operator):
    task_id: str
    msg: str = 'Hello World!'

    def execute(self, dag_context: DAGContext):
        print(self.msg)
        print(dag_context.ds)

if __name__ == '__main__':
    op = HelloWorldOperator(task_id='test_op')
    op.test()
