from airflow_plus.contrib.operators.sql_operators import DatabaseToFileSystem, ExecuteSQL
from airflow_plus.models import Component, OperatorInSubDAG

copy_into_snowflake = """
COPY INTO {{ table_name }}
SELECT $1
FROM @{{ stage }}/{{ path }}
"""


class DatabaseToS3ThenSnowflake(Component):
    def __init__(self, db_hook: str, sql: str, s3_hook: str, path: str):
        to_s3 = DatabaseToFileSystem(
            task_id='db_to_s3',
            db_hook=db_hook,
            sql=sql,
            fs_hook=s3_hook,
            path=path,
        )
        to_snowflake = ExecuteSQL(
            task_id='copy_into_snowflake',
            hook='snowflake',
            sql=copy_into_snowflake,
        )
        self.sub_dag = to_s3 >> to_snowflake

    @property
    def db_to_s3(self) -> OperatorInSubDAG:
        return self.sub_dag['db_to_s3']

    @property
    def copy_into_snowflake(self) -> OperatorInSubDAG:
        return self.sub_dag['copy_into_snowflake']
