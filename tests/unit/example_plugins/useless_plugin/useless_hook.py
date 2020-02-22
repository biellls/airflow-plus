from airflow.hooks.base_hook import BaseHook

from airflow_plus.models import CustomBaseHook


class UselessHook(BaseHook, CustomBaseHook):
    conn_type = 'useless'
    conn_type_long = 'Useless Test Hook'

    # noinspection PyMissingConstructor
    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    def do_nothing(self):
        print(f'{self.conn_id} is doing nothing')

    def get_conn(self):
        pass

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass


class EvenMoreUselessHook(CustomBaseHook):
    conn_type = 'more_useless'
    conn_type_long = 'Even More Useless'

    def __init__(self, conn_id: str):
        self.conn_id = conn_id


class UselessHookImplicitProtocol:
    conn_type = 'useless_implicit'
    conn_type_long = 'Useless Implicit'

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

