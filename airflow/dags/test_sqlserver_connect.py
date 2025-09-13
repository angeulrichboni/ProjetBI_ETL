from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_mssql_connect():
    # Remplace 'mssql_erp_bi' par l'ID de connexion configuré dans Airflow
    hook = MsSqlHook(mssql_conn_id='dw_erp_bi')
    
    try:
        conn = hook.get_conn()
        if conn:
            print("✅ Ping réussi : MSSQL est accessible.")
        else:
            print("❌ Le serveur MSSQL ne répond pas.")
        conn.close()
    except Exception as e:
        print(f"❌ Erreur lors du ping MSSQL : {e}")
        

dag = DAG(
    dag_id='test_mssql_connect',
    start_date=datetime(2025, 3, 25),
    schedule_interval='@once',
    catchup=False
)

test_conn_task = PythonOperator(
    task_id='test_mssql',
    python_callable=test_mssql_connect,
    dag=dag
)