from dataclasses import dataclass
from enum import Enum

import sqlparse
from airflow.hooks.dbapi_hook import DbApiHook

from airflow_plus.contrib import transformations
from airflow_plus.contrib.hooks.filesystem_hooks import FileSystemHook
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


class FileFormat(str, Enum):
    CSV = 'csv'
    JSON = 'json'


@dataclass
class DatabaseToFileSystem(Operator):
    template_fields = ('sql', 'path')
    template_ext = ('.sql',)

    task_id: str
    db_hook: DbApiHook
    sql: str
    fs_hook: FileSystemHook
    path: str
    format: FileFormat.CSV
    gzip: bool = False
    sql_parameters: dict = None

    def execute(self, dag_context: DAGContext):
        df = self.db_hook.get_pandas_df(self.sql, self.sql_parameters)
        if self.format == FileFormat.CSV:
            data = df.to_csv(index=False)
        elif self.format == FileFormat.JSON:
            data = df.to_json()
        else:
            assert False, f'Format case is not exhaustive. Missing "{self.format}"'
        if self.gzip:
            data = transformations.gzip_data(data)
        self.fs_hook.write_data(self.path, data)
