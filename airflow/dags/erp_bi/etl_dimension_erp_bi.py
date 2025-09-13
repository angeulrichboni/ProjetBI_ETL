from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from minio_utils import upload_to_minio, load_from_minio
from pathlib import Path


    

#Parametres de connexion
POSTGRES_CONN_ID = 'postgres_erp_bi'
MSSQL_CONN_ID = 'dw_erp_bi'


def extract_data():
    """ Extraction des données depuis PostgreSQL """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql_customers = "SELECT * FROM customers"
    sql_product_categories = "SELECT * FROM product_categories"
    sql_products = "SELECT * FROM products"
    sql_supplier = "SELECT * FROM suppliers"
    sql_warehouses = "SELECT * FROM warehouses"
    sql_dates = """
        SELECT DISTINCT order_date::DATE AS date from sales_orders
        UNION
        SELECT DISTINCT order_date::DATE AS date from purchase_orders
        UNION
        SELECT DISTINCT movement_date::DATE AS date from stock_movements
    """
    
    
    df_customers = pg_hook.get_pandas_df(sql_customers)
    df_product_categories = pg_hook.get_pandas_df(sql_product_categories)
    df_products = pg_hook.get_pandas_df(sql_products)
    df_suppliers = pg_hook.get_pandas_df(sql_supplier)
    df_warehouses = pg_hook.get_pandas_df(sql_warehouses)
    df_dates = pg_hook.get_pandas_df(sql_dates)
    
    # Enregistrer les données dans un fichier CSV
    # df_customers.to_csv(f'{TMP_DIR}/customers.csv', index=False)
    # df_product_categories.to_csv(f'{TMP_DIR}/product_categories.csv', index=False)
    # df_products.to_csv(f'{TMP_DIR}/products.csv', index=False)
    
    # Envoyer vers minio
    upload_to_minio(df_customers, 'customers.csv', folder='Dimension/Extraction')
    upload_to_minio(df_product_categories, 'product_categories.csv', folder='Dimension/Extraction')
    upload_to_minio(df_products, 'products.csv', folder='Dimension/Extraction')
    upload_to_minio(df_suppliers, 'suppliers.csv', folder='Dimension/Extraction')
    upload_to_minio(df_warehouses, 'warehouses.csv', folder='Dimension/Extraction')
    upload_to_minio(df_dates, 'dates.csv', folder='Dimension/Extraction')
    
    logging.info("Données extraites et envoyées vers MinIO avec succès.")
    
    
def transform_data():
    """ Transformation des données """
    # Charger les données extraites
    # df_customers = pd.read_csv(f'{TMP_DIR}/customers.csv')
    # df_product_categories = pd.read_csv(f'{TMP_DIR}/product_categories.csv')
    # df_products = pd.read_csv(f'{TMP_DIR}/products.csv')
    
    # Charger les données depuis MinIO
    df_customers = load_from_minio('customers.csv', folder='Dimension/Extraction')
    df_product_categories = load_from_minio('product_categories.csv', folder='Dimension/Extraction')
    df_products = load_from_minio('products.csv', folder='Dimension/Extraction')
    df_suppliers = load_from_minio('suppliers.csv', folder='Dimension/Extraction')
    df_warehouses = load_from_minio('warehouses.csv', folder='Dimension/Extraction')
    df_dates = load_from_minio('dates.csv', folder='Dimension/Extraction')
    
    # Renommer les colonnes
    df_customers.rename(columns={'id': 'CustomerID', 'name':'CustomerName', 
                                 'email': 'Email', 'phone':'Phone', 'address':'Address'}, inplace=True)
    df_product_categories.rename(columns={'id': 'CategoryID', 'name': 'CategoryName'}, inplace=True)
    df_products.rename(columns={'id': 'ProductID', 'name': 'ProductName', 'price': 'Price', 
                                'category_id': 'CategoryID', 
                                'cost':'Cost', 'uom':'UOM'}, inplace=True)
    df_suppliers.rename(columns={'id': 'SupplierID', 'name': 'SupplierName', 'contact_email': 'ContactEmail',
                                    'contact_phone': 'ContactPhone', 'address': 'Address'}, inplace=True)
    df_warehouses.rename(columns={'id': 'WarehouseID', 'name': 'WarehouseName', 'location': 'Location'}, inplace=True)
    
    df_dates['date'] = pd.to_datetime(df_dates['date'], errors='coerce')
    df_dates['Day'] = df_dates['date'].dt.day
    df_dates['Month'] = df_dates['date'].dt.month
    df_dates['Year'] = df_dates['date'].dt.year
    df_dates['Quarter'] = df_dates['date'].dt.quarter
    df_dates['Weekday'] = df_dates['date'].dt.weekday + 1  # Pour que le lundi soit 1 et le dimanche 7
    
    # Envouyer les donnees transformées vers MinIO
    upload_to_minio(df_customers, 'customers_transformed.csv', folder='Dimension/Transformation')
    upload_to_minio(df_product_categories, 'product_categories_transformed.csv', folder='Dimension/Transformation')
    upload_to_minio(df_products, 'products_transformed.csv', folder='Dimension/Transformation')
    upload_to_minio(df_suppliers, 'suppliers_transformed.csv', folder='Dimension/Transformation')
    upload_to_minio(df_warehouses, 'warehouses_transformed.csv', folder='Dimension/Transformation')
    upload_to_minio(df_dates, 'dates_transformed.csv', folder='Dimension/Transformation')
    
    logging.info("Données transformées et envoyées vers MinIO avec succès.")
    
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
    """Chargement des données dans SQL Server"""
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    
    # Charger les données transformées
    # df_customers = pd.read_csv(f'{TMP_DIR}/customers_transformed.csv')
    df_customers = load_from_minio('customers_transformed.csv', folder='Dimension/Transformation')
    for _, row in df_customers.iterrows():
        customer_name = escape_apostrophes(row['CustomerName'])
        email = escape_apostrophes(row['Email'])
        phone = escape_apostrophes(row['Phone'])
        address = escape_apostrophes(row['Address'])
        
        mssql_hook.run(f"""
            SET IDENTITY_INSERT DimCustomer ON;
            
            MERGE INTO DimCustomer AS target
            USING (SELECT {row['CustomerID']} AS CustomerID) AS source
            ON target.CustomerID = source.CustomerID
            WHEN MATCHED THEN
                UPDATE SET CustomerName = '{customer_name}',
                           Email = '{email}',
                           Phone = '{phone}',
                           Address = '{address}'
            WHEN NOT MATCHED THEN
                INSERT (CustomerID, CustomerName, Email, Phone, Address)
                VALUES ({row['CustomerID']}, '{customer_name}', '{email}', '{phone}', '{address}');
            
            SET IDENTITY_INSERT DimCustomer OFF;
        """)
        
    logging.info("Données clients chargées avec succès.")

    # df_product_categories = pd.read_csv(f'{TMP_DIR}/product_categories_transformed.csv')
    df_product_categories = load_from_minio('product_categories_transformed.csv', folder='Dimension/Transformation')
    for _, row in df_product_categories.iterrows():
        category_name = escape_apostrophes(row['CategoryName'])
        
        mssql_hook.run(f"""
            SET IDENTITY_INSERT DimProductCategory ON;
            
            MERGE INTO DimProductCategory AS target
            USING (SELECT {row['CategoryID']} AS CategoryID) AS source
            ON target.CategoryID = source.CategoryID
            WHEN MATCHED THEN
                UPDATE SET CategoryName = '{category_name}'
            WHEN NOT MATCHED THEN
                INSERT (CategoryID, CategoryName)
                VALUES ({row['CategoryID']}, '{category_name}');
            
            SET IDENTITY_INSERT DimProductCategory OFF;
        """)
    logging.info("Données catégories de produits chargées avec succès.")

    # df_products = pd.read_csv(f'{TMP_DIR}/products_transformed.csv')
    df_products = load_from_minio('products_transformed.csv', folder='Dimension/Transformation')
    for _, row in df_products.iterrows():
        product_name = escape_apostrophes(row['ProductName'])
        uom = escape_apostrophes(row['UOM'])

        mssql_hook.run(f"""
            SET IDENTITY_INSERT DimProduct ON;
            
            MERGE INTO DimProduct AS target
            USING (SELECT {row['ProductID']} AS ProductID) AS source
            ON target.ProductID = source.ProductID
            WHEN MATCHED THEN
                UPDATE SET ProductName = '{product_name}',
                           Price = {row['Price']},
                           CategoryID = {row['CategoryID']},
                           Cost = {row['Cost']},
                           UOM = '{uom}'
            WHEN NOT MATCHED THEN
                INSERT (ProductID, ProductName, Price, CategoryID, Cost, UOM)
                VALUES ({row['ProductID']}, '{product_name}', {row['Price']}, {row['CategoryID']}, {row['Cost']}, '{uom}');
                
            SET IDENTITY_INSERT DimProduct OFF;
        """)
    logging.info("Données produits chargées avec succès.")
        
    # Loading des données pour les fournisseurs
    df_suppliers = load_from_minio('suppliers_transformed.csv', folder='Dimension/Transformation')
    for _, row in df_suppliers.iterrows():
        supplier_name = escape_apostrophes(row['SupplierName'])
        contact_email = escape_apostrophes(row['ContactEmail'])
        contact_phone = escape_apostrophes(row['ContactPhone'])
        address = escape_apostrophes(row['Address'])
        
        mssql_hook.run(f"""
            SET IDENTITY_INSERT DimSupplier ON;
            
            MERGE INTO DimSupplier AS target
            USING (SELECT {row['SupplierID']} AS SupplierID) AS source
            ON target.SupplierID = source.SupplierID
            WHEN MATCHED THEN
                UPDATE SET SupplierName = '{supplier_name}',
                           ContactEmail = '{contact_email}',
                           ContactPhone = '{contact_phone}',
                           Address = '{address}'
            WHEN NOT MATCHED THEN
                INSERT (SupplierID, SupplierName, ContactEmail, ContactPhone, Address)
                VALUES ({row['SupplierID']}, '{supplier_name}', '{contact_email}', '{contact_phone}', '{address}');
                
            SET IDENTITY_INSERT DimSupplier OFF;
        """)
    logging.info("Données fournisseurs chargées avec succès.")
        
    # Loading des données pour les entrepots
    df_warehouses = load_from_minio('warehouses_transformed.csv', folder='Dimension/Transformation')
    for _, row in df_warehouses.iterrows():
        warehouse_name = escape_apostrophes(row['WarehouseName'])
        location = escape_apostrophes(row['Location'])
        
        mssql_hook.run(f"""
            SET IDENTITY_INSERT DimWarehouse ON;
            
            MERGE INTO DimWarehouse AS target
            USING (SELECT {row['WarehouseID']} AS WarehouseID) AS source
            ON target.WarehouseID = source.WarehouseID
            WHEN MATCHED THEN
                UPDATE SET WarehouseName = '{warehouse_name}',
                           Location = '{location}'
            WHEN NOT MATCHED THEN
                INSERT (WarehouseID, WarehouseName, Location)
                VALUES ({row['WarehouseID']}, '{warehouse_name}', '{location}');
                
            SET IDENTITY_INSERT DimWarehouse OFF;
        """)
    logging.info("Données entrepôts chargées avec succès.")
        
    # Charger la table DimDate
    df_dates = load_from_minio('dates_transformed.csv', folder='Dimension/Transformation')
    for _, row in df_dates.iterrows():
        date = validate_date(row['date'])
        if date is None:
            print(f"Invalid date detected: skipping row.")
            continue

        mssql_hook.run(f"""
            MERGE INTO DimDate AS target
            USING (SELECT '{date}' AS Date) AS source
            ON target.Date = source.Date
            WHEN MATCHED THEN
                UPDATE SET Day = {row['Day']},
                           Month = {row['Month']},
                           Year = {row['Year']},
                           Quarter = {row['Quarter']},
                           Weekday = {row['Weekday']}
            WHEN NOT MATCHED THEN
                INSERT (Date, Day, Month, Year, Quarter, Weekday)
                VALUES ('{date}', {row['Day']}, {row['Month']}, {row['Year']}, {row['Quarter']}, {row['Weekday']});
        """)
    logging.info("Données DimDate chargées avec succès.")
    
    
    
    

# send_success_email_dim = EmailOperator(
#     task_id="send_success_email_dim",
#     to=EMAIL_TO,  # Ton adresse e-mail
#     subject="ETL Dimension ERP BI - Succès ✅ - Exécution du {{ ds }}",
#     html_content=email_template_path_dim.read_text(encoding='utf-8'),
#     dag=dag,
# )