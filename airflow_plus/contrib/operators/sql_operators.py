from dataclasses import dataclass

import sqlparse
from airflow.hooks.dbapi_hook import DbApiHook

from airflow_plus.models import Operator, DAGContext


@dataclass
class ExecuteSQL(Operator):
    template_fields = ('sql',)
    template_ext = ('.sql',)

    task_id: str
    hook: DbApiHook
    sql: str
    autocommit: bool = True
    parameters: dict = None

    def execute(self, dag_context: DAGContext):
        self.hook.run(sqlparse.split(self.sql), autocommit=self.autocommit, parameters=self.parameters)
