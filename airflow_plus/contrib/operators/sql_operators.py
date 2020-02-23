from dataclasses import dataclass

import sqlparse
from airflow.hooks.dbapi_hook import DbApiHook

from airflow_plus.contrib.hooks.filesystem_hooks import FileSystemHook
from airflow_plus.contrib.data_utils import FileFormat, Compression
from airflow_plus.models import Operator, DAGContext


@dataclass
class ExecuteSQL(Operator):
    template_fields = ('sql',)
    template_ext = ('.sql',)

    task_id: str
    hook: DbApiHook
    sql: str
    autocommit: bool = True
    sql_parameters: dict = None

    def execute(self, dag_context: DAGContext):
        self.hook.run(sqlparse.split(self.sql), autocommit=self.autocommit, parameters=self.sql_parameters)


@dataclass
class DatabaseToFileSystem(Operator):
    template_fields = ('sql', 'path')
    template_ext = ('.sql',)

    task_id: str
    db_hook: DbApiHook
    sql: str
    fs_hook: FileSystemHook
    path: str
    format: FileFormat = FileFormat.CSV
    compression: Compression = Compression.NONE
    sql_parameters: dict = None

    def execute(self, dag_context: DAGContext):
        df = self.db_hook.get_pandas_df(self.sql, self.sql_parameters)
        data = self.format.convert_from_dataframe(df)
        data = self.compression.compress(data)
        self.fs_hook.write_data(self.path, data)
