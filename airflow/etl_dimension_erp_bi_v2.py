from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from minio_utils import upload_to_minio, load_from_minio
from engine_sql_utils import get_postgres_engine, get_mssql_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path    

#Parametres de connexion
EMAIL_TO = 'angeulrich890@gmail.com'

def send_email_on_failure(context):
    email_template_path = Path(__file__).parent / "templates/email_failure_etl.html"
    subject = f"ETL Dimension ERP BI - Échec - {context['task_instance'].task_id} - {context['ds']}"
    
    
    send_email = EmailOperator(
        task_id='send_email_on_failure',
        to=EMAIL_TO,
        subject=subject,
        html_content=email_template_path.read_text(encoding='utf-8'),
        dag=context['dag']
    )
    send_email.execute(context=context)
    

# Definition des paramètres de la DAG
default_args = {
    'owner': 'angeulrich',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['angeulrich890@gmail.com'],
    'email_on_failure': True,
    'email_on_success': True
}

dag = DAG(
    'etl_erp_bi_dimensions',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2 * * 5',
    start_date=datetime(2025, 3, 25),
    max_active_runs=1,
    tags=['ETL', 'ERP', 'BI']
)
# End of the DAG definition

def extract_data():
    """ Extraction des données depuis PostgreSQL """
    # Utiliser l'engine PostgreSQL pour la connexion
    engine = get_postgres_engine()
    
    if engine is None:
        logging.error("Erreur de connexion à PostgreSQL.")
        return
    
    # Definition des requetes SQL
    sql_customers = "SELECT * FROM customers"
    sql_product_categories = "SELECT * FROM product_categories"
    sql_products = "SELECT * FROM products"
    sql_supplier = "SELECT * FROM suppliers"
    sql_warehouses = "SELECT * FROM warehouses"
    
    try:
        # Executer et recuperer les données
        with engine.connect() as connection:
            result_customers = connection.execute(text(sql_customers))
            result_product_categories = connection.execute(text(sql_product_categories))
            result_products = connection.execute(text(sql_products))
            result_supplier = connection.execute(text(sql_supplier))
            result_warehouses = connection.execute(text(sql_warehouses))
            
            
            # Convertir les résultats en DataFrame
            df_customers = pd.DataFrame(result_customers.fetchall(), columns=result_customers.keys())
            df_product_categories = pd.DataFrame(result_product_categories.fetchall(), columns=result_product_categories.keys())
            df_products = pd.DataFrame(result_products.fetchall(), columns=result_products.keys())
            df_supplier = pd.DataFrame(result_supplier.fetchall(), columns=result_supplier.keys())
            df_warehouses = pd.DataFrame(result_warehouses.fetchall(), columns=result_warehouses.keys())
            
        
        logging.info("Données extraites avec succès depuis PostgreSQL.")
        
        # Envoyer vers minio
        upload_to_minio(df_customers, 'customers.csv', folder='Extraction')
        upload_to_minio(df_product_categories, 'product_categories.csv', folder='Extraction')
        upload_to_minio(df_products, 'products.csv', folder='Extraction')
        upload_to_minio(df_supplier, 'suppliers.csv', folder='Extraction')
        upload_to_minio(df_warehouses, 'warehouses.csv', folder='Extraction')
        
        logging.info("Données extraites et envoyées vers MinIO avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données : {e}")
    
    
 # Transformation des données   
def transform_data():
    """ Transformation des données """
    
    # Charger les données depuis MinIO
    try:
        df_customers = load_from_minio('customers.csv', folder='Extraction')
        df_product_categories = load_from_minio('product_categories.csv', folder='Extraction')
        df_products = load_from_minio('products.csv', folder='Extraction')
        df_supplier = load_from_minio('suppliers.csv', folder='Extraction')
        df_warehouses = load_from_minio('warehouses.csv', folder='Extraction')
        
        logging.info("Données chargées depuis MinIO avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors du chargement des données depuis MinIO : {e}")
        return
    
    # Transformation des données
    try:
        # Renommer les colonnes
        df_customers.rename(columns={'id': 'CustomerID', 'name':'CustomerName', 
                                    'email': 'Email', 'phone':'Phone', 'address':'Address'}, inplace=True)
        df_product_categories.rename(columns={'id': 'CategoryID', 'name': 'CategoryName'}, inplace=True)
        df_products.rename(columns={'id': 'ProductID', 'name': 'ProductName', 'price': 'Price', 
                                    'category_id': 'CategoryID', 
                                    'cost':'Cost', 'uom':'UOM', 'created_at':'CreatedAt', 'updated_at':'UpdatedAt'}, inplace=True)
        df_supplier.rename(columns={'id': 'SupplierID', 'name': 'SupplierName', 'contact_email': 'ContactEmail',
                                    'contact_phone': 'ContactPhone', 'address': 'Address'}, inplace=True)
        df_warehouses.rename(columns={'id': 'WarehouseID', 'name': 'WarehouseName', 'location': 'Location'}, inplace=True)
        
        logging.info("Données transformées avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de la transformation des données : {e}")
        return

    # Envouyer les donnees transformées vers MinIO
    try:
        upload_to_minio(df_customers, 'customers_transformed.csv', folder='Transformation')
        upload_to_minio(df_product_categories, 'product_categories_transformed.csv', folder='Transformation')
        upload_to_minio(df_products, 'products_transformed.csv', folder='Transformation')
        upload_to_minio(df_supplier, 'suppliers_transformed.csv', folder='Transformation')
        upload_to_minio(df_warehouses, 'warehouses_transformed.csv', folder='Transformation')
        
        logging.info("Données transformées et envoyées vers MinIO avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi des données transformées vers MinIO : {e}")
    
def escape_apostrophes(text):
    """Échappe les apostrophes dans les chaînes de texte"""
    return text.replace("'", "''")

def validate_date(date_str):
    """Valide et formate les dates au format YYYY-MM-DD HH:MM:SS"""
    try:
        return pd.to_datetime(date_str, errors='raise').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None  # Retourne None si la date est invalide
    
def load_data():
    """Chargement des données dans SQL Server avec SQLAlchemy"""
    
    # Récupérer l'engine MSSQL
    engine = get_mssql_engine()
    if not engine:
        logging.error("Échec de la connexion à SQL Server.")
        return

    try:
        # Charger les données transformées depuis MinIO
        df_customers = load_from_minio('customers_transformed.csv', folder='Transformation')
        df_product_categories = load_from_minio('product_categories_transformed.csv', folder='Transformation')
        df_products = load_from_minio('products_transformed.csv', folder='Transformation')
        df_supplier = load_from_minio('suppliers_transformed.csv', folder='Transformation')
        df_warehouses = load_from_minio('warehouses_transformed.csv', folder='Transformation')
        
        logging.info("Données transformées chargées avec succès depuis MinIO.")
        
        
        with engine.connect() as connection:
            try:
                # Insérer les données dans les tables respectives
                df_customers.to_sql('DimCustomer', con=connection, if_exists='replace', index=False, method='multi')
                
                logging.info("Données chargées avec succès dans SQL Server.")
            except SQLAlchemyError as e:
                logging.error(f"Erreur lors de l'insertion des données dans SQL Server : {e}")
                return

    except Exception as e:
        logging.error(f"Erreur lors du chargement des données depuis MinIO : {e}")
        return

    
        




extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    on_failure_callback=send_email_on_failure,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    on_failure_callback=send_email_on_failure,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    on_failure_callback=send_email_on_failure,
    dag=dag
)

email_template_path = Path(__file__).parent / "templates/email_success_etl.html"
send_success_email = EmailOperator(
    task_id="send_success_email",
    to=EMAIL_TO,  # Ton adresse e-mail
    subject="ETL Dimension ERP BI - Succès ✅ - Exécution du {{ ds }}",
    html_content=email_template_path.read_text(encoding='utf-8'),
    dag=dag,
)

# Définir les dépendances entre les tâches
extract_data_task >> transform_data_task >> load_data_task >> send_success_email