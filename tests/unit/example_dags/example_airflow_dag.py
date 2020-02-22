from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

with DAG(dag_id='airflow_test_dag', schedule_interval='@once', start_date=datetime.min) as dag:
    op = PythonOperator(task_id='op1', python_callable=lambda **kwargs: print('testing'))
    dag >> op

globals()[dag.dag_id] = dag

with DAG(dag_id='airflow_test_dag2', schedule_interval='@once', start_date=datetime.min) as dag:
    op = PythonOperator(task_id='op12', python_callable=lambda **kwargs: print('testing 2'))
    dag >> op
