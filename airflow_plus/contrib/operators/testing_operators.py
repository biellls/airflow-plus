from dataclasses import dataclass

from airflow_plus.models import Operator, DAGContext


@dataclass
class PrintOperator(Operator):
    template_fields = ('message',)

    message: str

    def execute(self, dag_context: DAGContext):
        print(self.message)
