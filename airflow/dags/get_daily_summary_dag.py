from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="call_postgres_procedure",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",  
    catchup=False
):

    call_sp = SQLExecuteQueryOperator(
        task_id="get_daily_summary",
        conn_id="my_postgres_conn",
        sql="CALL analytics.get_daily_summary();"
    )