import contextlib
import datetime

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable as v

from airflow.decorators import dag
from pandas import DataFrame
import pendulum

import vertica_python
import psycopg2

def load_stg_table(
    dag_start_dt: str,
    postgre_get_table_expr,
    vertica_del_from_table_expr,
    vertica_load_table_expr
):
    postgre_conn = psycopg2.connect(
        database    = v.get("postgre_db"), 
        user        = v.get('postgre_user'), 
        password    = v.get('postgre_pass'), 
        host        = v.get("postgre_host"), 
        port        = v.get("postgre_port")
    )
    vertica_conn = vertica_python.connect(
        host        = v.get("vert_host"),
        port        = v.get("vert_port"),
        user        = v.get("vert_user"),
        password    = v.get("vert_pass")
    )
    #PG
    with open(postgre_get_table_expr, "r") as file:
        postgre_get_table_expr_sql = file.read()
        
    with contextlib.closing(postgre_conn.cursor()) as cur:
        cur.execute(postgre_get_table_expr_sql, {'dag_start_dt': dag_start_dt})
        remaining_rows = DataFrame(cur.fetchall())
    postgre_conn.close()
    
    #dframe ops
    df = DataFrame(remaining_rows)
    df = df.drop_duplicates()
    num_rows = len(df)
    chunk_size = num_rows // 10
    
    #VERTICA
    with open(vertica_del_from_table_expr, "r") as file:
        vertica_del_from_table_expr_sql = file.read()
    
    with open(vertica_load_table_expr, "r") as file:
        vertica_load_table_expr_sql = file.read()
        
    with contextlib.closing(vertica_conn.cursor()) as cur:
        cur.execute(vertica_del_from_table_expr_sql, [dag_start_dt])
        row_start = 0
        while row_start <= num_rows:
            row_end = min(row_start + chunk_size, num_rows)
            
            df.loc[row_start: row_end].to_csv('/tmp/chunk.csv', index=False, header=False)
            
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(vertica_load_table_expr_sql, chunk, buffer_size=65536)
            vertica_conn.commit()
            
            print(f"loaded {row_start}-{row_end}")
            row_start += chunk_size + 1
    
    vertica_conn.close()
    
def load_dds_table(
    dag_start_dt: str,
    vertica_del_from_table_expr,
    vertica_load_table_expr
):
    vertica_conn = vertica_python.connect(
        host        = v.get("vert_host"),
        port        = v.get("vert_port"),
        user        = v.get("vert_user"),
        password    = v.get("vert_pass")
    )
    with open(vertica_del_from_table_expr, "r") as file:
        vertica_del_from_table_expr_sql = file.read()
    
    with open(vertica_load_table_expr, "r") as file:
        vertica_load_table_expr_sql = file.read()
        
    with contextlib.closing(vertica_conn.cursor()) as cur:
        cur.execute(vertica_del_from_table_expr_sql, [dag_start_dt])
        cur.execute(vertica_load_table_expr_sql, [dag_start_dt])
        vertica_conn.commit()
        
    vertica_conn.close()
    
dag = DAG(
    dag_id          = "final_project_dag",
    schedule        = "@daily",
    catchup         = True,
    start_date      = pendulum.parse('2022-10-01'), 
    end_date        = pendulum.parse('2022-10-31'),
    max_active_runs = 1,
    default_args    = {
        'owner':'airflow',
        'retries':1,
        'retry_delay': timedelta (seconds = 60)
    }
)
with dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    with TaskGroup(group_id='load_dds') as load_dds:
        with TaskGroup(group_id='load_transactions') as load_transactions:
            load_transactions_from_pg_to_vertica = PythonOperator(
                task_id='load_transactions_from_pg_to_vertica',
                python_callable=load_stg_table,
                op_kwargs={
                    'dag_start_dt': '{{ds}}',
                    'postgre_get_table_expr': '/lessons/dags/repository/postgre/dml/postgre_get_transactions.sql',
                    'vertica_del_from_table_expr': '/lessons/dags/repository/vertica/dml/stg/vert_stg_del_transactions.sql',
                    'vertica_load_table_expr': '/lessons/dags/repository/vertica/dml/stg/vert_stg_load_transactions.sql'
                }
            )
            
            load_transactions_stg_to_dds = PythonOperator(
                task_id='load_transactions_stg_to_dds',
                python_callable=load_dds_table,
                op_kwargs={
                    'dag_start_dt': '{{ds}}',
                    'vertica_del_from_table_expr': '/lessons/dags/repository/vertica/dml/dds/vert_dds_del_transactions.sql',
                    'vertica_load_table_expr': '/lessons/dags/repository/vertica/dml/dds/vert_dds_load_transactions.sql'
                }
            )
            
            load_transactions_from_pg_to_vertica >> load_transactions_stg_to_dds
        
        with TaskGroup(group_id='load_currencies') as load_currencies:
            load_currencies_from_pg_to_vertica = PythonOperator(
                task_id='load_currencies_from_pg_to_vertica',
                python_callable=load_stg_table,
                op_kwargs={
                    'dag_start_dt': '{{ds}}',
                    'postgre_get_table_expr': '/lessons/dags/repository/postgre/dml/postgre_get_currencies.sql',
                    'vertica_del_from_table_expr': '/lessons/dags/repository/vertica/dml/stg/vert_stg_del_currencies.sql',
                    'vertica_load_table_expr': '/lessons/dags/repository/vertica/dml/stg/vert_stg_load_currencies.sql'
                }
            )
            
            load_currencies_stg_to_dds = PythonOperator(
                task_id='load_currencies_stg_to_dds',
                python_callable=load_dds_table,
                op_kwargs={
                    'dag_start_dt': '{{ds}}',
                    'vertica_del_from_table_expr': '/lessons/dags/repository/vertica/dml/dds/vert_dds_del_currencies.sql',
                    'vertica_load_table_expr': '/lessons/dags/repository/vertica/dml/dds/vert_dds_load_currencies.sql'
                }
            )
            
            load_currencies_from_pg_to_vertica >> load_currencies_stg_to_dds
            
    with TaskGroup(group_id='load_cdm') as load_cdm:
        global_metrics = PythonOperator(
            task_id='global_metrics',
            python_callable=load_dds_table,
            op_kwargs={
                'dag_start_dt': '{{ds}}',
                'vertica_del_from_table_expr': '/lessons/dags/repository/vertica/dml/dds/vert_dds_del_global_metrics.sql',
                'vertica_load_table_expr': '/lessons/dags/repository/vertica/dml/dds/vert_dds_load_global_metrics.sql'
            }
        )  
    
start >> load_dds >> load_cdm >> end