import boto3
from io import BytesIO
from airflow.models import Variable
import requests
import logging
import pandas as pd


# Récupérer les variables MinIO depuis l'Airflow UI MIONIO_ACCESS_KEY, 
MINIO_ENDPOINT = Variable.get("minio_endpoint_url")
MIONIO_ACCESS_KEY = Variable.get("minio_access_key_erpbi")
MIONIO_SECRET_KEY = Variable.get("minio_secret_key_erpbi")

# MINIO_ENDPOINT = "http://192.168.10.210:9100"  # Remplace par l'URL de ton MinIO
# ACCESS_KEY = "angeulrich"
# SECRET_KEY = "Server#storage@2k24"

# Connexion à MinIO
minio_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MIONIO_ACCESS_KEY,
    aws_secret_access_key=MIONIO_SECRET_KEY,
    region_name='us-east-1',
) 

BUCKET_NAME = 'my-erpbi-data-lake'

# Fonction pour générer une Presigned URL
def generate_presigned_url(object_name, folder=None, expiration=3600):
    if folder:
        object_name_full = f"{folder}/{object_name}"
    try:
        url = minio_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET_NAME, "Key": object_name_full},
            ExpiresIn=expiration,
        )
        logging.info(f"Presigned URL générée pour {object_name_full}: {url}")
        return url
    except Exception as e:
        logging.error(f"Erreur lors de la génération de la Presigned URL pour {object_name_full}: {e}")
        return None

# fonction pour uploader un fichier sur MinIO
def upload_to_minio(df, object_name, folder):
    # Créer un chemin complet pour l'objet (avec le stockage spécifique)
    object_name_full = f"{folder}/{object_name}"
    
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    try:
        minio_client.put_object(
            Bucket=BUCKET_NAME,
            Key=object_name_full,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        logging.info(f"Fichier {object_name_full} uploadé avec succès sur MinIO.")
    except Exception as e:
        logging.error(f"Erreur lors de l'upload du fichier {object_name_full}: {e}")

# Fonction pour télécharger un fichier depuis MinIO via Presigned URL
def load_from_minio(object_name, folder=None):
    logging.info(f"Téléchargement du fichier {object_name} depuis MinIO...")

    presigned_url = generate_presigned_url(object_name, folder=folder)
    if not presigned_url:
        raise Exception(f"Impossible de générer l'URL signée pour {object_name}")

    try:
        response = requests.get(presigned_url)
        response.raise_for_status()  # Lève une erreur en cas d'échec du téléchargement
        csv_buffer = BytesIO(response.content)
        df = pd.read_csv(csv_buffer)
        logging.info(f"Fichier {object_name} téléchargé avec succès via Presigned URL.")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors du téléchargement du fichier {object_name} : {e}")
        raise