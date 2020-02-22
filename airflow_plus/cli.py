from argparse import Namespace

import airflow
import click
from airflow.bin.cli import webserver

from airflow_plus.airflow_testing import mock_airflow_db
from airflow_plus.models import Connection


@click.group()
def cli():
    pass


@cli.command(name='webserver')
@click.option('--hostname', default='0.0.0.0')
@click.option('--port', type=click.INT, default=8080)
@click.option('--debug', is_flag=True, default=False)
@click.option('--mock-db', is_flag=True, default=False)
def cli_webserver(hostname: str, port: int, debug: bool, mock_db: bool):
    run_modded_webserver(hostname, port, debug, mock_db)


class Args(Namespace):
    def __init__(self, **kwargs):
        self.args = kwargs or {}

    def __getattr__(self, item):
        return self.args.get(item)


def run_modded_webserver(hostname: str, port: int, debug: bool, mock_db: bool):
    for cls in Connection.custom_hook_classes():
        conn_type = getattr(cls, 'conn_type', None)
        if conn_type:
            conn_type_long = getattr(cls, 'conn_type_long', None)
            # noinspection PyProtectedMember
            airflow.models.Connection._types.append((conn_type, conn_type_long or conn_type))

    args = Args(hostname=hostname, port=port, debug=debug)
    if mock_db:
        with mock_airflow_db():
            webserver(args)
    else:
        webserver(args)
