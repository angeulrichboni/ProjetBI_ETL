from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.email import EmailOperator
from pathlib import Path
from minio_utils import upload_to_minio, load_from_minio
import logging
from datetime import datetime, timedelta
import pandas as pd
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np


#Parametres de connexion
POSTGRES_CONN_ID = 'postgres_erp_bi'
MSSQL_CONN_ID = 'dw_erp_bi'
BATCH_SIZE = 100000  # Taille des lots pour le traitement
DATE_FORMAT = '%m/%d/%Y'  # Format standard des dates


def get_data_in_batches(query, batch_size=BATCH_SIZE):
    # Connexion unique à PostgreSQL pour toute la durée de l'extraction
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    
    # Curseur côté serveur pour itérer sur les résultats par lots
    cursor = conn.cursor(name='server_side_cursor')  # Curseur côté serveur
    cursor.itersize = batch_size  # Nombre de lignes à récupérer par itération

    try:
        # Exécute la requête sur le curseur
        cursor.execute(query)
        
        # Récupère les données par petits lots
        while True:
            records = cursor.fetchmany(batch_size)
            if not records:
                break
            # Création d'un DataFrame à partir des enregistrements récupérés
            df_batch = pd.DataFrame(records, columns=[desc[0] for desc in cursor.description])
            yield df_batch
    except Exception as e:
        # Log des erreurs éventuelles
        logging.error(f"Erreur lors de l'extraction des données : {e}")
    finally:
        # Fermeture du curseur et de la connexion
        cursor.close()
        conn.close()

@lru_cache(maxsize=None)
def get_date_mapping():
    """Cache des correspondances Date -> DateID depuis DimDate"""
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    existing_dates = mssql_hook.get_pandas_df("SELECT DateID, Date FROM DimDate")
    existing_dates['Date'] = pd.to_datetime(existing_dates['Date']).dt.strftime(DATE_FORMAT)
    return dict(zip(existing_dates['Date'], existing_dates['DateID']))

def process_batch(df_batch, date_column, date_mapping, output_name):
    """Traite un lot de données et renomme les colonnes selon les tables FactSales, FactPurchase, etc."""
    # Conversion des dates
    if date_column:
        df_batch[date_column] = pd.to_datetime(df_batch[date_column], errors='coerce').dt.strftime(DATE_FORMAT)
        
        # Mapping des DateID
        id_column = f"{date_column}ID"
        df_batch[id_column] = df_batch[date_column].map(date_mapping)
        
        # Vérification des dates manquantes
        if df_batch[id_column].isnull().any():
            missing_dates = df_batch[df_batch[id_column].isnull()][date_column].unique()
            raise ValueError(f"Dates manquantes dans DimDate: {missing_dates.tolist()}")
    
    # Initialiser un dictionnaire pour le renommage des colonnes
    rename_dict = {}
    additional_columns = []  # Liste pour les colonnes supplémentaires à ajouter
    
    # Renommage des colonnes selon le type de données
    if output_name == 'ventes':
        rename_dict = {
            'salesorderid': 'SalesOrderID',
            'product_id': 'ProductID',
            'customer_id': 'CustomerID',
            'order_dateID': 'OrderDate',
            'quantity': 'Quantity',
            'unit_price': 'UnitPrice'
        }
        # Calculer TotalPrice AVANT le renommage, en utilisant les noms originaux des colonnes
        if 'unit_price' in df_batch.columns and 'quantity' in df_batch.columns:
            df_batch['TotalPrice'] = df_batch['quantity'] * df_batch['unit_price']
            additional_columns = ['TotalPrice']
    
    elif output_name == 'achats':
        rename_dict = {
            'purchaseorderid': 'PurchaseOrderID',
            'supplier_id': 'SupplierID',
            'product_id': 'ProductID',
            'order_dateID': 'OrderDate',
            'quantity': 'Quantity',
            'unit_cost': 'UnitCost'
        }
        # Calculer TotalCost AVANT le renommage, en utilisant les noms originaux des colonnes
        if 'unit_cost' in df_batch.columns and 'quantity' in df_batch.columns:
            df_batch['TotalCost'] = df_batch['quantity'] * df_batch['unit_cost']
            additional_columns = ['TotalCost']
    
    elif output_name == 'stock':
        rename_dict = {
            'id': 'StockID',
            'product_id': 'ProductID',
            'warehouse_id': 'WarehouseID',
            'quantity': 'Quantity',
            'min_quantity': 'MinQuantity',
            'max_quantity': 'MaxQuantity'
        }
    
    elif output_name == 'mouvements':
        rename_dict = {
            'id': 'MovementID',
            'product_id': 'ProductID',
            'warehouse_id': 'WarehouseID',
            'movement_type': 'MovementType',
            'quantity': 'Quantity',
            'movement_dateID': 'MovementDate',
            'reference': 'Reference'
        }
    
    # Renommage des colonnes
    df_batch = df_batch.rename(columns=rename_dict)

    # Créer un nouveau DataFrame uniquement avec les colonnes renommées et les colonnes supplémentaires pertinentes
    renamed_columns_df = df_batch[list(rename_dict.values()) + additional_columns]

    # Sauvegarde du DataFrame transformé dans un fichier CSV avec les nouvelles entêtes
    file_name = f"{output_name}_transformed.csv"
    renamed_columns_df.to_csv(file_name, index=False)
    
    return renamed_columns_df

def process_in_batches(df, date_column, output_name):
    """Traitement d'un DataFrame par lots"""
    date_mapping = get_date_mapping() if date_column else None
    
    results = []
    
    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE].copy()
        
        # Appel à process_batch pour traiter le lot
        processed_batch = process_batch(batch, date_column, date_mapping, output_name)
        
        results.append(processed_batch)
        
        # Log de progression
        if (i // BATCH_SIZE) % 10 == 0:
            logging.info(f"Traitement de {output_name} - lot {i // BATCH_SIZE + 1}")
    
    return pd.concat(results)
### Fonction d'extraction des données

def extract_data():
    """Extraction des données depuis PostgreSQL avec optimisation des connexions"""
    queries = {
        'sales': """
            SELECT so.id AS SalesOrderID, so.customer_id, sol.product_id, 
                   sol.quantity, sol.unit_price, so.order_date
            FROM sales_orders so
            JOIN sales_order_lines sol ON so.id = sol.order_id
        """,
        'purchase': """
            SELECT po.id AS PurchaseOrderID, po.supplier_id, pol.product_id, 
                   pol.quantity, pol.unit_cost, po.order_date
            FROM purchase_orders po
            JOIN purchase_order_lines pol ON po.id = pol.order_id
        """,
        'stock': "SELECT * FROM stock",
        'stock_movements': "SELECT * FROM stock_movements"
    }
    
    # Traitement pour chaque type de données
    for name, query in queries.items():
        # Utilisation du générateur pour récupérer les données par lots
        for df_batch in get_data_in_batches(query):
            # Conversion des dates si nécessaire
            if 'order_date' in df_batch.columns:
                df_batch['order_date'] = pd.to_datetime(df_batch['order_date']).dt.date
            elif 'movement_date' in df_batch.columns:
                df_batch['movement_date'] = pd.to_datetime(df_batch['movement_date']).dt.date
            
            # Envoi vers MinIO
            upload_to_minio(df_batch, f'{name}.csv', folder='Fact/Extraction')
    
    logging.info("Données extraites et envoyées vers MinIO avec succès.")


### Fonction de transformation des données    
def transform_data():
    """Transformation optimisée avec traitement par lots et cache"""
    try:
        # 1. Chargement parallèle initial
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_sales = executor.submit(load_from_minio, 'sales.csv', folder='Fact/Extraction')
            future_purchase = executor.submit(load_from_minio, 'purchase.csv', folder='Fact/Extraction')
            future_stock_movements = executor.submit(load_from_minio, 'stock_movements.csv', folder='Fact/Extraction')
            future_stock = executor.submit(load_from_minio, 'stock.csv', folder='Fact/Extraction')
            
            df_sales = future_sales.result()
            df_purchase = future_purchase.result()
            df_stock_movements = future_stock_movements.result()
            df_stock = future_stock.result()

        # 2. Traitement par lots en parallèle
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_sales = executor.submit(process_in_batches, df_sales, 'order_date', 'ventes')
            future_purchase = executor.submit(process_in_batches, df_purchase, 'order_date', 'achats')
            future_stock_movements = executor.submit(process_in_batches, df_stock_movements, 'movement_date', 'mouvements')
            future_stock = executor.submit(process_in_batches, df_stock, None, 'stock')
            
            df_sales_processed = future_sales.result()
            df_purchase_processed = future_purchase.result()
            df_stock_movements_processed = future_stock_movements.result()
            df_stock_processed = future_stock.result()

        # 3. Sauvegarde parallèle des résultats
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(upload_to_minio, df_sales_processed, 'sales_transformed.csv', 'Fact/Transformation')
            executor.submit(upload_to_minio, df_purchase_processed, 'purchase_transformed.csv', 'Fact/Transformation')
            executor.submit(upload_to_minio, df_stock_movements_processed, 'stock_movements_transformed.csv', 'Fact/Transformation')
            executor.submit(upload_to_minio, df_stock_processed, 'stock_transformed.csv', 'Fact/Transformation')

        logging.info("Transformation terminée avec succès")
        
    except Exception as e:
        logging.error(f"Erreur lors de la transformation: {str(e)}")
        raise

### Fonction de chargement des données dans MSSQL
def load_data():
    """Chargement des données dans MSSQL avec INSERT tout en gérant les clés auto-incrémentées."""
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

    try:
        # 1️⃣ Charger les données depuis MinIO en parallèle
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_sales = executor.submit(load_from_minio, 'sales_transformed.csv', folder='Fact/Transformation')
            future_purchase = executor.submit(load_from_minio, 'purchase_transformed.csv', folder='Fact/Transformation')
            future_stock = executor.submit(load_from_minio, 'stock_transformed.csv', folder='Fact/Transformation')
            future_stock_movement = executor.submit(load_from_minio, 'stock_movements_transformed.csv', folder='Fact/Transformation')

            df_sales = future_sales.result()
            df_purchase = future_purchase.result()
            df_stock = future_stock.result()
            df_stock_movement = future_stock_movement.result()

        # 2️⃣ Vérification des colonnes attendues
        REQUIRED_COLUMNS = {
            'FactSales': ['SalesOrderID', 'CustomerID', 'ProductID', 'OrderDate', 'Quantity', 'UnitPrice', 'TotalPrice'],
            'FactPurchase': ['PurchaseOrderID', 'SupplierID', 'ProductID', 'OrderDate', 'Quantity', 'UnitCost', 'TotalCost'],
            'FactStock': ['StockID', 'ProductID', 'WarehouseID', 'Quantity', 'MinQuantity', 'MaxQuantity'],
            'FactStockMovement': ['MovementID', 'ProductID', 'WarehouseID', 'MovementType', 'Quantity', 'MovementDate', 'Reference']
        }

        dfs = {
            'FactSales': df_sales,
            'FactPurchase': df_purchase,
            'FactStock': df_stock,
            'FactStockMovement': df_stock_movement
        }

        for table_name, df in dfs.items():
            missing_cols = [col for col in REQUIRED_COLUMNS[table_name] if col not in df.columns]
            extra_cols = [col for col in df.columns if col not in REQUIRED_COLUMNS[table_name]]
            if missing_cols:
                raise ValueError(f"❌ Colonnes manquantes pour {table_name}: {missing_cols}")
            if extra_cols:
                logging.warning(f"⚠️ Colonnes supplémentaires ignorées pour {table_name}: {extra_cols}")

        # 3️⃣ Fonction pour insérer les données en excluant les ID auto-incrémentés
        def execute_insert(table_name, df, id_column):
            conn = mssql_hook.get_conn()
            cursor = conn.cursor()

            try:
                # ✅ Exclure la colonne ID auto-incrémentée
                columns = [col for col in df.columns if col != id_column]  
                insert_cols = ", ".join([f"[{col}]" for col in columns])

                # ✅ Préparer la requête SQL sans ID
                insert_sql = f"""
                    INSERT INTO {table_name} ({insert_cols})
                    VALUES ({', '.join(['%s'] * len(columns))});
                """

                # ✅ Préparer les données et gérer les valeurs NULL
                df = df.astype(object).where(pd.notna(df), None)
                tuples = [tuple(row) for row in df[columns].to_numpy()]  # Exclure l'ID

                # ✅ Exécuter l'INSERT
                cursor.executemany(insert_sql, tuples)
                conn.commit()
                logging.info(f"✅ INSERT réussi pour {table_name}")

            except Exception as e:
                logging.error(f"❌ Erreur lors de l'INSERT dans {table_name}: {str(e)}")
                conn.rollback()
                raise

            finally:
                cursor.close()
                conn.close()

        # 4️⃣ Configuration des tables (en précisant les ID à exclure)
        table_configs = [
            {'table_name': 'FactSales', 'df': df_sales, 'id_column': 'SalesOrderID'},
            {'table_name': 'FactPurchase', 'df': df_purchase, 'id_column': 'PurchaseOrderID'},
            {'table_name': 'FactStock', 'df': df_stock, 'id_column': 'StockID'},
            {'table_name': 'FactStockMovement', 'df': df_stock_movement, 'id_column': 'MovementID'}
        ]

        # 5️⃣ Exécution en parallèle
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(execute_insert, config['table_name'], config['df'], config['id_column'])
                for config in table_configs
            ]

            for future in futures:
                future.result()

        logging.info("🚀 Chargement des données terminé avec succès !")

    except Exception as e:
        logging.error(f"❌ Erreur lors du chargement: {str(e)}")
        raise