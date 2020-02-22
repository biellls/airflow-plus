import os
from pathlib import Path

from airflow import settings
from airflow.models import DagBag

from airflow_plus.airflow_testing import mock_airflow_db
from airflow_plus.cli import run_modded_webserver

if __name__ == '__main__':
    os.environ['AIRFLOW_HOME'] = str(Path(__file__).parent / 'test_airflow_home')

    os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'false'
    os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'false'
    with mock_airflow_db() as db:
        # settings.PLUGINS_FOLDER = str(Path(__file__).parent / 'example_plugins')
        # settings.DAGS_FOLDER = str(Path(__file__).parent / 'example_dags')
        settings.prepare_syspath()
        db.set_connection(
            conn_id='default_useless',
            conn_type='useless',
        )

        dag_bag = DagBag(
            dag_folder=str(Path(__file__).parent/'example_dags'),
            include_examples=False,
        )
        run_modded_webserver(hostname='0.0.0.0', port=8080, debug=True, mock_db=False)
