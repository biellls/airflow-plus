import sqlite3
from contextlib import closing
from datetime import datetime

from pytest import fixture


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
from airflow_plus.airflow_testing import mock_airflow_db
from airflow_plus.contrib.operators.sql_operators import DatabaseToFileSystem


@fixture
def tmp_sqlite(tmp_path):
    sqlite_db = str(tmp_path / 'test.db')
    with closing(sqlite3.connect(sqlite_db)) as conn, closing(conn.cursor()) as cursor:
        cursor.execute('CREATE TABLE stocks (date text, trans text, symbol text, qty real, price real)')
        insert_query = 'INSERT INTO stocks VALUES (?,?,?,?,?)'
        insert_queries_params = [
            ('2006-01-05', 'BUY', 'RHAR', 11, 35.14),
            ('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
            ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
            ('2006-04-06', 'SELL', 'IBM', 500, 53.00),
        ]
        for params in insert_queries_params:
            cursor.execute(insert_query, params)
        conn.commit()
    print(f'Initialised test SQLite db in {sqlite_db}')
    yield sqlite_db


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
def test_database_to_filesystem_operator(tmp_sqlite, tmp_path):
    with mock_airflow_db() as af_db:
        print(f'Adding sqlite connection...')
        af_db.set_connection(
            conn_id='source_database',
            conn_type='sqlite',
            host=tmp_sqlite,
        )
        print(f'Adding data lake connection with base path {tmp_path}...')
        af_db.set_connection(
            conn_id='data_lake',
            conn_type='local_filesystem',
            extra={
                'base_path': str(tmp_path)
            }
        )

        op = DatabaseToFileSystem(
            task_id='testing_db_to_fs',
            db_hook='source_database',
            sql='SELECT * FROM stocks',
            fs_hook='data_lake',
            path='dumps/{{ ds }}/stocks.csv'
        )
        op.test(execution_date=datetime(2020, 2, 18), airflow_db=af_db)
        print((tmp_path / 'dumps/2020-02-18/stocks.csv').read_text())
