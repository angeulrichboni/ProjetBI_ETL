from sqlalchemy import create_engine, Table, MetaData, select
from airflow.hooks.base import BaseHook
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
import logging

# Paramètres de connexion
POSTGRES_CONN_ID = 'postgres_erp_bi'
MSSQL_CONN_ID = 'dw_erp_bi'

# Configurer le logger pour enregistrer les erreurs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

## Fonction pour obtenir le moteur SQLAlchemy pour PostgreSQL
def get_postgres_engine():
    """Retourne un moteur SQLAlchemy pour PostgreSQL"""
    try:
        # Récupérer la connexion depuis Airflow
        conn = BaseHook.get_connection(POSTGRES_CONN_ID)
        
        # Vérification si la connexion contient les informations nécessaires
        if not conn.login or not conn.password or not conn.host or not conn.port or not conn.schema:
            raise ValueError("Les informations de connexion PostgreSQL sont manquantes.")
        
        # Encoder le mot de passe pour éviter les problèmes avec les caractères spéciaux
        encoded_password = quote_plus(conn.password)
        
        # Construire la chaîne de connexion PostgreSQL
        connection_string = f"postgresql://{conn.login}:{encoded_password}@{conn.host}:{conn.port}/{conn.schema}"
        engine = create_engine(connection_string)
        
        logger.info("Connexion PostgreSQL réussie.")
        return engine
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la connexion PostgreSQL : {e}")
        return None

## Fonction pour obtenir le moteur SQLAlchemy pour SQL Server
def get_mssql_engine():
    """Retourne un moteur SQLAlchemy pour SQL Server"""
    try:
        # Récupérer la connexion depuis Airflow
        conn = BaseHook.get_connection(MSSQL_CONN_ID)
        
        # Vérification si la connexion contient les informations nécessaires
        if not conn.login or not conn.password or not conn.host or not conn.port or not conn.schema:
            raise ValueError("Les informations de connexion MSSQL sont manquantes.")
        
        # Encoder le mot de passe pour éviter les problèmes avec les caractères spéciaux
        encoded_password = quote_plus(conn.password)
        
        # Construire la chaîne de connexion SQL Server
        connection_string = f"mssql+pyodbc://{conn.login}:{encoded_password}@{conn.host}:{conn.port}/{conn.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    
        engine = create_engine(connection_string)
        
        logger.info("Connexion SQL Server réussie.")
        return engine
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la connexion MSSQL : {e}")
        return None
    
    






# Exemple d'utilisation des fonctions pour obtenir les moteurs et exécuter une requête

# Test pour SQL Server
# engine = get_mssql_engine()
# print(engine)

# # Créer un objet MetaData pour interagir avec la base de données
# metadata = MetaData()

# # Définir la table (ici, nous utilisons DimCustomer, mais vous pouvez adapter si nécessaire)
# customers_table = Table('DimCustomer', metadata, autoload_with=engine)

# # Vérifier si l'objet engine est valide avant de continuer
# if engine:
#     try:
#         # Utilisation de la gestion des connexions avec "with"
#         with engine.connect() as connection:
#             # Création de la requête avec SQLAlchemy Expression Language
#             stmt = select(customers_table.c.CustomerName).limit(1)
            
#             # Exécution de la requête
#             result = connection.execute(stmt)
            
#             # Récupérer la première ligne du résultat
#             print(result.fetchone())
#     except SQLAlchemyError as e:
#         print(f"Erreur lors de l'exécution de la requête : {e}")
# else:
#     print("La connexion à la base de données a échoué.")

# Test pour PostgreSQL
# engine = get_postgres_engine()
# print(engine)

# # Création d'une instance MetaData (pour interagir avec la structure de la base de données)
# metadata = MetaData()

# # Définition de la table avec l'objet Table
# customers_table = Table('customers', metadata, autoload_with=engine)

# # Vérification si l'engine est valide
# if engine:
#     try:
#         with engine.connect() as connection:
#             # Création de la requête en utilisant l'objet Table et l'expression select()
#             stmt = select(customers_table).limit(1)
            
#             # Exécution de la requête
#             result = connection.execute(stmt)
            
#             # Récupération de la première ligne du résultat
#             print(result.fetchone())
#     except SQLAlchemyError as e:
#         print(f"Erreur lors de l'exécution de la requête : {e}")
# else:
#     print("La connexion à la base de données a échoué.")
