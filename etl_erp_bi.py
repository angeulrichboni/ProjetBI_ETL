import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from airflow import DAG
from etl_facts_erp_bi import extract_data as extract_fact, transform_data as transform_fact, load_data as load_fact
from etl_dimension_erp_bi import extract_data as extract_dim, transform_data as transform_dim, load_data as load_dim
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from pathlib import Path
import time
from pendulum import timezone

EMAIL_TO = 'angeulrich890@gmail.com'
email_template_path_dim = Path(__file__).parent / "templates/email_success_etl_dim.html"
email_template_path_fact = Path(__file__).parent / "templates/email_success_etl_fact.html"
email_template_path_start = Path(__file__).parent / "templates/start_pipeline.html"
email_template_path_end = Path(__file__).parent / "templates/end_pipeline.html"

## Email en cas d'erreur
def send_email_on_failure_dim(context):
    email_template_path = Path(__file__).parent / "templates/email_failure_etl_dim.html"
    subject = f"ETL Dimension ERP BI - Échec - {context['task_instance'].task_id} - {context['ds']}"
    
    send_email = EmailOperator(
        task_id='send_email_on_failure',
        to=EMAIL_TO,
        subject=subject,
        html_content=email_template_path.read_text(encoding='utf-8'),
        dag=context['dag']
    )
    send_email.execute(context=context)

def send_email_on_failure_fact(context):
    email_template_path = Path(__file__).parent / "templates/email_failure_etl_fact.html"
    subject = f"ETL FACT ERP BI - Échec - {context['task_instance'].task_id} - {context['ds']}"
    
    send_email = EmailOperator(
        task_id='send_email_on_failure',
        to=EMAIL_TO,
        subject=subject,
        html_content=email_template_path.read_text(encoding='utf-8'),
        dag=context['dag']
    )
    send_email.execute(context=context)

def pause_execution():
    time.sleep(60)
    
# Configuration par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #'start_date': datetime(2025, 4, 4, 9, 20, tzinfo=timezone('UTC')),
    'start_date': datetime.now(tz=timezone('UTC')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG maître
dag = DAG(
    'erpbi_etl_master',
    description='DAG maître pour le traitement ETL des dimensions et des faits',
    default_args=default_args,
    schedule_interval='10 09 * * 5',  # Lancement manuel ou déclenché par un autre événement
    catchup=False,
    tags=['erp_bi', 'master', 'ETL', 'trigger', 'dim', 'fact', 'BI'],
)
    
# 🔹 1. Début du pipeline avec un DummyOperator (ou tâche d'initialisation)
start_pipeline = EmailOperator(
    task_id="start_pipeline_email",
    to=EMAIL_TO,  # Ton adresse e-mail
    subject="Début du pipeline ETL des faits et dimensions du projet ERP BI - {{ ds }}",
    html_content=email_template_path_start.read_text(encoding='utf-8'),
    dag=dag,
)

#🔹 2. ETL des tables de dimension
extract_dimension = PythonOperator(
    task_id='extract_dimension',
    python_callable=extract_dim,
    dag=dag,
    on_failure_callback=send_email_on_failure_dim,
    provide_context=True,
)
transform_dimension = PythonOperator(
    task_id='transform_dimension',
    python_callable=transform_dim,
    dag=dag,
    on_failure_callback=send_email_on_failure_dim,
    provide_context=True,
)
load_dimension = PythonOperator(
    task_id='load_dimension',
    python_callable=load_dim,
    dag=dag,
    on_failure_callback=send_email_on_failure_dim,
    provide_context=True,
)

#🔹 3. Pause
pause_task = PythonOperator(
    task_id='pause_execution',
    python_callable=pause_execution,
    dag=dag,
    on_failure_callback=send_email_on_failure_dim,
    provide_context=True,
)

#🔹 4. ETL des tables de faits
extract_fact_table = PythonOperator(
    task_id='extract_fact_table',
    python_callable=extract_fact,
    dag=dag,
    on_failure_callback=send_email_on_failure_fact,
    provide_context=True,
)
transform_fact_table = PythonOperator(
    task_id='transform_fact_table',
    python_callable=transform_fact,
    dag=dag,
    on_failure_callback=send_email_on_failure_fact,
    provide_context=True,
)
load_fact_table = PythonOperator(
    task_id='load_fact_table',
    python_callable=load_fact,
    dag=dag,
    on_failure_callback=send_email_on_failure_fact,
    provide_context=True,
)

#🔹 5. Fin du pipeline avec un DummyOperator (ou tâche de nettoyage )
end_pipeline = EmailOperator(
    task_id="end_pipeline_email",
    to=EMAIL_TO,  # Ton adresse e-mail
    subject="Fin du pipeline ETL des faits et dimensions du projet ERP BI - {{ ds }}",
    html_content=email_template_path_end.read_text(encoding='utf-8'),
    dag=dag,
)

#🔹 6. Definition des emails
send_success_email_dim = EmailOperator(
    task_id="send_success_email_dim",
    to=EMAIL_TO,  # Ton adresse e-mail
    subject="ETL Dimension ERP BI - Succès ✅ - Exécution du {{ ds }}",
    html_content=email_template_path_dim.read_text(encoding='utf-8'),
    dag=dag,
)
send_success_email_fact = EmailOperator(
    task_id="send_success_email_fact",
    to=EMAIL_TO,  # Ton adresse e-mail
    subject="ETL FACT ERP BI - Succès ✅ - Exécution du {{ ds }}",
    html_content=email_template_path_fact.read_text(encoding='utf-8'),
    dag=dag,
)

# 🔹 7. Définition des dépendances entre les tâches

start_pipeline >> extract_dimension >> transform_dimension >> load_dimension >> send_success_email_dim

send_success_email_dim >> pause_task >> extract_fact_table >> transform_fact_table >> load_fact_table >> send_success_email_fact

# Les deux branches doivent finir avant la fin du pipeline
[send_success_email_dim, send_success_email_fact] >> end_pipeline
    