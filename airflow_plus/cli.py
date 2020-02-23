import shutil
from argparse import Namespace
from pathlib import Path

import airflow
import click
from airflow.bin.cli import webserver
from airflow.models import Connection

from airflow_plus.airflow_testing import mock_airflow_db
from airflow_plus.models import ConnectionHelper


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


def replace_logos():
    airflow_static_path = Path(airflow.__file__).parent/'www_rbac/static'
    airflow_plus_static_path = Path(__file__).parent/'static'
    for logo_png in (airflow_plus_static_path/'logos').rglob('*.png'):
        af_logo_png = airflow_static_path / str(logo_png.name)
        assert af_logo_png.exists()
        af_logo_png_backup = af_logo_png.with_suffix('.png.old')
        if not af_logo_png_backup.exists():
            af_logo_png.rename(af_logo_png_backup)
            shutil.copy(str(logo_png), str(af_logo_png))


def run_modded_webserver(hostname: str, port: int, debug: bool, mock_db: bool):
    # replace_logos()
    for cls in ConnectionHelper.custom_hook_classes():
        conn_type = getattr(cls, 'conn_type', None)
        if conn_type:
            conn_type_long = getattr(cls, 'conn_type_long', None)
            # noinspection PyProtectedMember
            Connection._types = [(conn_type, conn_type_long or conn_type)] + Connection._types

    args = Args(hostname=hostname, port=port, debug=debug)
    if mock_db:
        with mock_airflow_db():
            webserver(args)
    else:
        webserver(args)
