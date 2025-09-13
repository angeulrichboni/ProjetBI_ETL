from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
import logging

# Configuration de l'adresse e-mail du destinataire
EMAIL_TO = "angeulrich890@gmail.com"

def test_postgresql_connect(**kwargs):
    hook = PostgresHook(postgres_conn_id='postgres_erp_bi')
    
    try:
        conn = hook.get_conn()
        if conn:
            message = "✅ Connexion réussie : PostgreSQL est accessible."
        else:
            message = "❌ Le serveur PostgreSQL ne répond pas."
        conn.close()
    except Exception as e:
        message = f"❌ Erreur lors du test de connexion PostgreSQL : {e}"
        
    
    # Log du resultat
    logging.info(message)
    
    # Stocker le message pour l'utiliser dans l'EmailOperator
    kwargs['ti'].xcom_push(key='connection_result', value=message)
        
        

dag = DAG(
    dag_id='test_postgresql_connect',
    start_date=datetime(2025, 3, 25),
    schedule_interval='@once',
    catchup=False
)

test_conn_task = PythonOperator(
    task_id='test_postgres',
    python_callable=test_postgresql_connect,
    dag=dag
)

# Task pour envoyer un email avec le résultat
send_email_task = EmailOperator(
    task_id='send_email',
    to=EMAIL_TO,
    subject='Résultat du test de connexion PostgreSQL',
    html_content='{{ ti.xcom_pull(task_ids="test_postgres", key="connection_result") }}',
    dag=dag
)

# Définir l'ordre d'exécution des tâches
test_conn_task >> send_email_task