import shutil
import sys
from argparse import Namespace
from pathlib import Path
from typing import List, Optional

import airflow
import click
from airflow import settings
from airflow.bin.cli import webserver, process_subdir
from airflow.models import Connection, DagBag, DagModel
from click import ClickException
from tabulate import tabulate
from termcolor import colored

from airflow_plus.airflow_testing import mock_airflow_db, set_airflow_db
from airflow_plus.models import ConnectionHelper
from airflow_plus.remotes import Remotes


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


@cli.group(name='remote')
def cli_remote():
    """Manage $AIRFLOW_HOME/.remotes config"""
    pass


@cli_remote.command(name='add')
@click.argument('remote')       # No autocomplete because the remote is new
@click.option('--sql-alchemy-conn')
@click.option('--fernet-key')
def remote_add(remote: str, sql_alchemy_conn: str, fernet_key: str):
    """Add a remote for deployments and management"""
    Remotes.add_remote(remote, sql_alchemy_conn, fernet_key)
    print(f'Added remote {remote}')


@cli_remote.command(name='ls')
@click.option('-l', '--long', is_flag=True, default=False)
def remote_list(long: bool):
    """List configured airflow remotes"""
    if long:
        header = ['REMOTE_NAME', 'SQL_ALCHEMY_CONN', 'FERNET_KEY']
        table_body = [
            [remote, Remotes.sql_alchemy_conn(remote), Remotes.fernet_key(remote)]
            for remote in Remotes.remote_names
        ]
        print(tabulate(table_body, header, 'plain'))
    else:
        for remote in Remotes.remote_names:
            print(remote)


def get_remote_names(ctx, args, incomplete) -> List[str]:
    return [x for x in Remotes.remote_names if incomplete in x]


@cli_remote.command(name='rm')
@click.argument('remote', autocompletion=get_remote_names)
def remote_rm(remote: str):
    """Remove remote"""
    Remotes.remove_remote(remote)
    print(f'Removed remote {remote}')


@cli.group(name='dag')
def cli_dags():
    """Manage Airflow DAGs"""
    pass


@cli_dags.command(name='ls')
@click.argument('remote', autocompletion=get_remote_names, required=False, default=None)
@click.option('-l', '--long', is_flag=True, default=False)
def list_dags(remote: Optional[str], long: bool):
    if not remote:
        dags_folder = str(Path(settings.AIRFLOW_HOME) / 'dags')
        dag_bag = DagBag(process_subdir(dags_folder))
        if dag_bag.import_errors:
            raise ClickException(f'Error listing dags. Found some import errors: {dag_bag.import_errors}')
        dag_ids = [v.dag_id for k, v in dag_bag.dags.items()]
    else:
        with set_airflow_db(Remotes.sql_alchemy_conn(remote), Remotes.fernet_key(remote)):
            dags = settings.Session().query(DagModel).filter(DagModel.is_active)
            dag_ids = [d.dag_id for d in dags]

    if not long:
        for dag_id in dag_ids:
            print(dag_id)
        if not remote:
            for dag_id, dag in dag_bag.import_errors.items():
                print(colored(dag_id, 'red'), file=sys.stderr)


if __name__ == '__main__':
    cli()
