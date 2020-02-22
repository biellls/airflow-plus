from argparse import Namespace
from pathlib import Path

from airflow import settings
from airflow.bin.cli import webserver
from airflow.models import Connection

from airflow_plus.airflow_testing import mock_airflow_db


# class Args(Namespace):
#     def __init__(self, **kwargs):
#         self.args = kwargs or {}
#
#     def __getattr__(self, item):
#         return self.args.get(item)
from airflow_plus.cli import run_modded_webserver

if __name__ == '__main__':
    # Connection._types.append(('ZZZZZZZ', 'HACK!!!'))
    with mock_airflow_db():
        settings.PLUGINS_FOLDER = str(Path(__file__).parent / 'example_plugins')
        settings.prepare_syspath()
        run_modded_webserver(hostname='0.0.0.0', port=8080, debug=True, mock_db=False)
        # args = Args(hostname='0.0.0.0', port=8080, debug=True)
        # webserver(args)
