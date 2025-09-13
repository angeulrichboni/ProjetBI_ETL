import boto3
from botocore.config import Config

# Configuration MinIO
MINIO_ENDPOINT = "https://apistorage.abkm.tech"  # Remplace par l'URL de ton MinIO
ACCESS_KEY = "1YefTVl9ioz2ua9Ks3ue"
SECRET_KEY = "Mgk1h6JzgxPGPJC6I79SOgsT1m1NIcfGdCSRegNt"
BUCKET_NAME = "my-erpbi-data-lake"  # Remplace par ton bucket MinIO

# Création du client S3 avec l'endpoint de MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,  # Spécifique à MinIO
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    # config=Config(signature_version="s3v4")
)

# Lister les objets du bucket
def list_objects(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in response:
            print(f"📂 Contenu du bucket '{bucket_name}':\n")
            for obj in response["Contents"]:
                print(f" - {obj['Key']} (Taille: {obj['Size']} octets)")
        else:
            print(f"🗂 Le bucket '{bucket_name}' est vide.")
    except Exception as e:
        print(f"❌ Erreur lors de la récupération des objets : {e}")

# Exécuter la fonction
if __name__ == "__main__":
    list_objects(BUCKET_NAME)
