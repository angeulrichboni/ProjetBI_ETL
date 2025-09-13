from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import hashlib
from minio_utils import upload_to_minio, load_from_minio
from engine_sql_utils import get_postgres_engine, get_mssql_engine
import logging
from sqlalchemy import text

# Utils
EMAIL_TO = 'angeulrich890@gmail.com'

# Deut de definition du DAG
default_args = {
    'owner': 'angeulrich',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['angeulrich890@gmail.com'],
    'email_on_failure': True,
    'email_on_success': True
}

dag = DAG(
    'pipeline_etl_dimension_erp_bi',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2 * * 5',
    start_date=datetime(2025, 3, 25)
)
# Fin de definition du DAG

def send_email_on_failure(context):
    send_email = EmailOperator(
        task_id='send_email_on_failure',
        to=EMAIL_TO,
        subject='ETL Dimension ERP BI - Echec',
        html_content=f"La tâche {context.get('task_instance').task_id} a échoué.",
        dag=dag
    )
    send_email.execute(context=context)
    
def calculate_data_hash(df):
    """Calcule un hash des données pour détecter les changements"""
    return hashlib.md5(pd.util.hash_pandas_object(df).values.tobytes()).hexdigest()

def check_for_changes():
    """Vérifie s'il y a des changements dans les données source"""
    # Connexion au bd via SQLAlchemy
    pg_engine = get_postgres_engine()
    mssql_engine = get_mssql_engine()
    
    # Dictionnaire pour stocker les résultats
    changes = {
        'customers': {'new_data': False, 'updates': False},
        'product_categories': {'new_data': False, 'updates': False},
        'products': {'new_data': False, 'updates': False}
    }
    
    # 1. Vérifier pour les clients
    df_customers_source = pd.read_sql("SELECT * FROM customers", con=pg_engine)
    df_customers_target = pd.read_sql("SELECT CustomerID, CustomerName, Email, Phone, Address FROM DimCustomer", con=mssql_engine)
    
    # Calculer les hashs pour détecter les changements
    source_hash = calculate_data_hash(df_customers_source)
    target_hash = calculate_data_hash(df_customers_target)
    
    if len(df_customers_source) > len(df_customers_target):
        changes['customers']['new_data'] = True
    if source_hash != target_hash:
        changes['customers']['updates'] = True
    
    # 2. Vérifier pour les catégories de produits
    df_categories_source = pd.read_sql("SELECT * FROM product_categories", con=pg_engine)
    df_categories_target = pd.read_sql("SELECT CategoryID, CategoryName FROM DimProductCategory", con=mssql_engine)
    
    source_hash = calculate_data_hash(df_categories_source)
    target_hash = calculate_data_hash(df_categories_target)
    
    if len(df_categories_source) > len(df_categories_target):
        changes['product_categories']['new_data'] = True
    if source_hash != target_hash:
        changes['product_categories']['updates'] = True
    
    # 3. Vérifier pour les produits
    df_products_source = pd.read_sql("SELECT * FROM products",  con=pg_engine)
    df_products_target = pd.read_sql("SELECT ProductID, ProductName, Price, CategoryID, Cost, UOM, CreatedAt, UpdatedAt FROM DimProduct", con=mssql_engine)
    
    source_hash = calculate_data_hash(df_products_source)
    target_hash = calculate_data_hash(df_products_target)
    
    if len(df_products_source) > len(df_products_target):
        changes['products']['new_data'] = True
    if source_hash != target_hash:
        changes['products']['updates'] = True
    
    # Sauvegarder les résultats pour les tâches suivantes
    return changes

def decide_what_to_do(**kwargs):
    """Décide quelles tâches exécuter en fonction des changements détectés"""
    ti = kwargs['ti']
    changes = ti.xcom_pull(task_ids='check_for_changes')
    
    tasks_to_execute = []
    
    # Vérifier s'il y a des changements
    has_changes = any(
        table_data['new_data'] or table_data['updates']
        for table_data in changes.values()
    )
    
    if not has_changes:
        return 'no_changes_task'
    
    # Ajouter les tâches nécessaires en fonction des changements
    tasks_to_execute.append('extract_data_task')
    tasks_to_execute.append('transform_data_task')
    
    # Vérifier quelles opérations de chargement sont nécessaires
    if any(table_data['new_data'] for table_data in changes.values()):
        tasks_to_execute.append('insert_data_task')
    if any(table_data['updates'] for table_data in changes.values()):
        tasks_to_execute.append('update_data_task')
    
    tasks_to_execute.append('send_success_email')
    
    return tasks_to_execute

def extract_data():
    """Extraction des données depuis PostgreSQL"""
    pg_engine = get_postgres_engine()
    
    sql_customers = "SELECT * FROM customers"
    sql_product_categories = "SELECT * FROM product_categories"
    sql_products = "SELECT * FROM products"
    
    df_customers = pd.read_sql(sql_customers, con=pg_engine)
    df_product_categories = pd.read_sql(sql_product_categories, con=pg_engine)
    df_products = pd.read_sql(sql_products, con=pg_engine)
    
    # Enregistrer les données dans un fichier CSV
    upload_to_minio(df_customers, 'customers.csv', folder='Extraction')
    upload_to_minio(df_product_categories, 'product_categories.csv', folder='Extraction')
    upload_to_minio(df_products, 'products.csv', folder='Extraction')
    logging.info("Données extraites et envoyées vers MinIO avec succès.")

def transform_data():
    """Transformation des données"""
    # Charger les données extraites
    df_customers = load_from_minio('customers.csv', folder='Extraction')
    df_product_categories = load_from_minio('product_categories.csv', folder='Extraction')
    df_products = load_from_minio('products.csv', folder='Extraction')
    
    # Renommer les colonnes
    df_customers.rename(columns={'id': 'CustomerID', 'name':'CustomerName', 
                               'email': 'Email', 'phone':'Phone', 'address':'Address'}, inplace=True)
    df_product_categories.rename(columns={'id': 'CategoryID', 'name': 'CategoryName'}, inplace=True)
    df_products.rename(columns={'id': 'ProductID', 'name': 'ProductName', 'price': 'Price', 
                              'category_id': 'CategoryID', 
                              'cost':'Cost', 'uom':'UOM', 'created_at':'CreatedAt', 'updated_at':'UpdatedAt'}, inplace=True)
    
    # Sauvegarder les données transformées
    upload_to_minio(df_customers, 'customers_transformed.csv', folder='Transformation')
    upload_to_minio(df_product_categories, 'product_categories_transformed.csv', folder='Transformation')
    upload_to_minio(df_products, 'products_transformed.csv', folder='Transformation')

def escape_apostrophes(text):
    """Échappe les apostrophes dans les chaînes de texte"""
    return text.replace("'", "''") if text else ''

def validate_date(date_str):
    """Valide et formate les dates au format YYYY-MM-DD HH:MM:SS"""
    try:
        return pd.to_datetime(date_str, errors='raise').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

def insert_data():
    """Insertion des nouvelles données dans SQL Server"""
    mssql_engine = get_mssql_engine()
    
    # Charger les données transformées
    df_customers = load_from_minio('customers_transformed.csv', folder='Transformation')
    df_product_categories = load_from_minio('product_categories_transformed.csv', folder='Transformation')
    df_products = load_from_minio('products_transformed.csv', folder='Transformation')
    
    # Obtenir les IDs existants
    existing_customers = pd.read_sql("SELECT CustomerID FROM DimCustomer", con=mssql_engine)['CustomerID'].tolist()
    existing_categories = pd.read_sql("SELECT CategoryID FROM DimProductCategory", con=mssql_engine)['CategoryID'].tolist()
    existing_products = pd.read_sql("SELECT ProductID FROM DimProduct", con=mssql_engine)['ProductID'].tolist()
    
    # Filtrer les nouvelles données
    new_customers = df_customers[~df_customers['CustomerID'].isin(existing_customers)]
    new_categories = df_product_categories[~df_product_categories['CategoryID'].isin(existing_categories)]
    new_products = df_products[~df_products['ProductID'].isin(existing_products)]
    
    # Insérer les nouveaux clients
    if not new_customers.empty:
        with mssql_engine.connect() as connection:
            for _, row in new_customers.iterrows():
                customer_name = escape_apostrophes(row['CustomerName'])
                email = escape_apostrophes(row['Email'])
                phone = escape_apostrophes(row['Phone'])
                address = escape_apostrophes(row['Address'])
                
                insert_query = text(f"""
                    SET IDENTITY_INSERT DimCustomer ON;
                    INSERT INTO DimCustomer (CustomerID, CustomerName, Email, Phone, Address)
                    VALUES (:CustomerID, :CustomerName, :Email, :Phone, :Address);
                    SET IDENTITY_INSERT DimCustomer OFF;
                """)
                
                # Exécuter la requête d'insertion
                connection.execute(insert_query, {
                    'CustomerID': row['CustomerID'],
                    'CustomerName': customer_name,
                    'Email': email,
                    'Phone': phone,
                    'Address': address
                })
    
    # Insérer les nouvelles catégories
    if not new_categories.empty:
        with mssql_engine.connect() as connection:
            for _, row in new_categories.iterrows():
                category_name = escape_apostrophes(row['CategoryName'])
                
                insert_query = text("""
                    SET IDENTITY_INSERT DimProductCategory ON;
                    INSERT INTO DimProductCategory (CategoryID, CategoryName)
                    VALUES (:CategoryID, :CategoryName);
                    SET IDENTITY_INSERT DimProductCategory OFF;
                """)
                
                connection.execute(insert_query, {
                    'CategoryID': row['CategoryID'],
                    'CategoryName': category_name
                })
    
    # Insertion des nouveaux produits
    if not new_products.empty:
        with mssql_engine.connect() as connection:  # Créer une connexion avec SQLAlchemy
            for _, row in new_products.iterrows():
                product_name = escape_apostrophes(row['ProductName'])
                uom = escape_apostrophes(row['UOM'])
                
                created_at = validate_date(row['CreatedAt'])
                updated_at = validate_date(row['UpdatedAt'])
                
                if created_at is None or updated_at is None:
                    continue
                
                # Requête paramétrée pour l'insertion d'un produit
                insert_query = text("""
                    SET IDENTITY_INSERT DimProduct ON;
                    INSERT INTO DimProduct (ProductID, ProductName, Price, CategoryID, Cost, UOM, CreatedAt, UpdatedAt)
                    VALUES (:ProductID, :ProductName, :Price, :CategoryID, :Cost, :UOM, :CreatedAt, :UpdatedAt);
                    SET IDENTITY_INSERT DimProduct OFF;
                """)
                
                # Exécution de la requête avec les paramètres
                connection.execute(insert_query, {
                    'ProductID': row['ProductID'],
                    'ProductName': product_name,
                    'Price': row['Price'],
                    'CategoryID': row['CategoryID'],
                    'Cost': row['Cost'],
                    'UOM': uom,
                    'CreatedAt': created_at,
                    'UpdatedAt': updated_at
                })
    

# Fonction pour la mise à jour des données
def update_data():
    """Mise à jour des données existantes dans SQL Server"""
    # Initialisation du moteur SQLAlchemy
    mssql_engine = get_mssql_engine()
    
    # Charger les données transformées
    df_customers = load_from_minio('customers_transformed.csv', folder='Transformation')
    df_product_categories = load_from_minio('product_categories_transformed.csv', folder='Transformation')
    df_products = load_from_minio('products_transformed.csv', folder='Transformation')
    
    # Obtenir les données actuelles
    current_customers = pd.read_sql("SELECT CustomerID, CustomerName, Email, Phone, Address FROM DimCustomer", con=mssql_engine)
    current_categories = pd.read_sql("SELECT CategoryID, CategoryName FROM DimProductCategory", con=mssql_engine)
    current_products = pd.read_sql("SELECT ProductID, ProductName, Price, CategoryID, Cost, UOM, CreatedAt, UpdatedAt FROM DimProduct", con=mssql_engine)
    
    # Fusionner pour trouver les différences
    merged_customers = pd.merge(df_customers, current_customers, on='CustomerID', suffixes=('_new', '_current'))
    merged_categories = pd.merge(df_product_categories, current_categories, on='CategoryID', suffixes=('_new', '_current'))
    merged_products = pd.merge(df_products, current_products, on='ProductID', suffixes=('_new', '_current'))
    
    # Filtrer les lignes avec des différences
    changed_customers = merged_customers[
        (merged_customers['CustomerName_new'] != merged_customers['CustomerName_current']) |
        (merged_customers['Email_new'] != merged_customers['Email_current']) |
        (merged_customers['Phone_new'] != merged_customers['Phone_current']) |
        (merged_customers['Address_new'] != merged_customers['Address_current'])
    ]
    
    changed_categories = merged_categories[
        (merged_categories['CategoryName_new'] != merged_categories['CategoryName_current'])
    ]
    
    changed_products = merged_products[
        (merged_products['ProductName_new'] != merged_products['ProductName_current']) |
        (merged_products['Price_new'] != merged_products['Price_current']) |
        (merged_products['CategoryID_new'] != merged_products['CategoryID_current']) |
        (merged_products['Cost_new'] != merged_products['Cost_current']) |
        (merged_products['UOM_new'] != merged_products['UOM_current']) |
        (merged_products['CreatedAt_new'] != merged_products['CreatedAt_current']) |
        (merged_products['UpdatedAt_new'] != merged_products['UpdatedAt_current'])
    ]
    
    # Mise à jour des clients modifiés
    if not changed_customers.empty:
        with mssql_engine.connect() as connection:
            for _, row in changed_customers.iterrows():
                customer_name = escape_apostrophes(row['CustomerName_new'])
                email = escape_apostrophes(row['Email_new'])
                phone = escape_apostrophes(row['Phone_new'])
                address = escape_apostrophes(row['Address_new'])
                
                update_query = text("""
                    UPDATE DimCustomer
                    SET CustomerName = :CustomerName,
                        Email = :Email,
                        Phone = :Phone,
                        Address = :Address
                    WHERE CustomerID = :CustomerID
                """)
                
                connection.execute(update_query, {
                    'CustomerName': customer_name,
                    'Email': email,
                    'Phone': phone,
                    'Address': address,
                    'CustomerID': row['CustomerID']
                })
    
    # Mise à jour des catégories modifiées
    if not changed_categories.empty:
        with mssql_engine.connect() as connection:
            for _, row in changed_categories.iterrows():
                category_name = escape_apostrophes(row['CategoryName_new'])
                
                update_query = text("""
                    UPDATE DimProductCategory
                    SET CategoryName = :CategoryName
                    WHERE CategoryID = :CategoryID
                """)
                
                connection.execute(update_query, {
                    'CategoryName': category_name,
                    'CategoryID': row['CategoryID']
                })
    
    # Mise à jour des produits modifiés
    if not changed_products.empty:
        with mssql_engine.connect() as connection:
            for _, row in changed_products.iterrows():
                product_name = escape_apostrophes(row['ProductName_new'])
                uom = escape_apostrophes(row['UOM_new'])
                
                created_at = validate_date(row['CreatedAt_new'])
                updated_at = validate_date(row['UpdatedAt_new'])
                
                if created_at is None or updated_at is None:
                    continue
                
                update_query = text("""
                    UPDATE DimProduct
                    SET ProductName = :ProductName,
                        Price = :Price,
                        CategoryID = :CategoryID,
                        Cost = :Cost,
                        UOM = :UOM,
                        CreatedAt = :CreatedAt,
                        UpdatedAt = :UpdatedAt
                    WHERE ProductID = :ProductID
                """)
                
                connection.execute(update_query, {
                    'ProductName': product_name,
                    'Price': row['Price_new'],
                    'CategoryID': row['CategoryID_new'],
                    'Cost': row['Cost_new'],
                    'UOM': uom,
                    'CreatedAt': created_at,
                    'UpdatedAt': updated_at,
                    'ProductID': row['ProductID']
                })
                

# Tâches
check_for_changes_task = PythonOperator(
    task_id='check_for_changes',
    python_callable=check_for_changes,
    dag=dag
)

decide_task = BranchPythonOperator(
    task_id='decide_what_to_do',
    python_callable=decide_what_to_do,
    provide_context=True,
    dag=dag
)

no_changes_task = DummyOperator(
    task_id='no_changes_task',
    dag=dag
)

extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id='insert_data_task',
    python_callable=insert_data,
    dag=dag
)

update_data_task = PythonOperator(
    task_id='update_data_task',
    python_callable=update_data,
    dag=dag
)

send_success_email = EmailOperator(
    task_id="send_success_email",
    to=EMAIL_TO,
    subject="ETL Dimension ERP BI - Success ✅",
    html_content="<h3>Le pipeline ETL a été exécuté avec succès !</h3>",
    dag=dag,
)


# Définir les dépendances entre les tâches
check_for_changes_task >> decide_task
decide_task >> no_changes_task
decide_task >> extract_data_task >> transform_data_task
transform_data_task >> insert_data_task >> send_success_email
transform_data_task >> update_data_task >> send_success_email