import contextlib
from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable as v
import pendulum

from airflow.decorators import dag
import vertica_python

def init_dds_tables(
    init_tables_expr
):
    vertica_conn = vertica_python.connect(
        host        = v.get("vert_host"),
        port        = v.get("vert_port"),
        user        = v.get("vert_user"),
        password    = v.get("vert_pass")
    )
    with open(init_tables_expr, "r") as file:
        sql = file.read()
        
    with contextlib.closing(vertica_conn.cursor()) as cur:
        cur.execute(sql)
        vertica_conn.commit()
        
    vertica_conn.close()
    
dag = DAG(
    dag_id          = "final_project_init_dag",
    schedule        = "@once",
    start_date      = pendulum.parse('2022-10-01'), 
    max_active_runs = 1
)
with dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    init_tables = PythonOperator(
        task_id='init_tables',
        python_callable=init_dds_tables,
        op_kwargs={
            'init_tables_expr': '/lessons/dags/repository/vertica/ddl/init_tables.sql'
        }
    )
    
start >> init_tables >> end