import contextlib
import json
import os
import tempfile
from typing import Optional, Union, ContextManager, List

from airflow import models
from airflow import settings
from airflow.models import Connection, Variable
from airflow.utils.db import initdb
from airflow.utils.db import merge_conn
from cryptography.fernet import Fernet


@contextlib.contextmanager
def set_env(**environ):
    """
    Temporarily set the process environment variables.
    :param environ: Environment variables to set
    """
    old_environ = dict(os.environ)
    os.environ.update(environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


class AirflowDb:
    sql_alchemy_conn: str = None

    def __init__(self, sql_alchemy_conn: str):
        self.sql_alchemy_conn = sql_alchemy_conn

    def set_connection(
            self,
            conn_id: str,
            conn_type: str,
            host: Optional[str] = None,
            schema: Optional[str] = None,
            login: Optional[str] = None,
            password: Optional[str] = None,
            port: Optional[int] = None,
            extra: Optional[Union[str, dict]] = None,
    ):
        assert repr(settings.engine.url) == self.sql_alchemy_conn
        session = settings.Session()
        new_conn = Connection(conn_id=conn_id, conn_type=conn_type, host=host,
                              login=login, password=password, schema=schema, port=port)
        if extra is not None:
            new_conn.set_extra(extra if isinstance(extra, str) else json.dumps(extra))

        session.add(new_conn)
        session.commit()
        session.close()

    def set_variable(
            self,
            var_id: str,
            value: str,
            is_encrypted: Optional[bool] = None
    ):
        assert repr(settings.engine.url) == self.sql_alchemy_conn
        session = settings.Session()
        new_var = Variable(key=var_id, _val=value, is_encrypted=is_encrypted)
        session.add(new_var)
        session.commit()
        session.close()

    def list_connections(self) -> List[str]:
        assert repr(settings.engine.url) == self.sql_alchemy_conn
        session = settings.Session()
        return [x.conn_id for x in session.query(Connection)]


@contextlib.contextmanager
def mock_airflow_db() -> ContextManager[AirflowDb]:
    with tempfile.TemporaryDirectory() as temp_dir:
        test_db_path = os.path.join(temp_dir, 'airflow.db')
        sql_alchemy_conn = f'sqlite:///{test_db_path}'
        with set_airflow_db(sql_alchemy_conn, Fernet.generate_key().decode()):
            initdb()
            yield AirflowDb(sql_alchemy_conn=sql_alchemy_conn)


@contextlib.contextmanager
def set_airflow_db(sql_alchemy_conn: str, fernet_key: str):
    with set_env(
            AIRFLOW__CORE__SQL_ALCHEMY_CONN=sql_alchemy_conn,
            AIRFLOW__CORE__FERNET_KEY=fernet_key,
    ):
        settings.configure_vars()
        settings.configure_orm()
        assert repr(settings.engine.url) == sql_alchemy_conn
        yield
    settings.configure_vars()
    settings.configure_orm()


def set_connection(conn: dict):
    merge_conn(models.Connection(**conn))
