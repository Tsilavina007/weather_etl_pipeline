import logging

from airflow.models import Variable
from supabase import create_client, Client

# Config Supabase
SUPABASE_URL = Variable.get("SUPABASE_URL")
SUPABASE_KEY = Variable.get("SUPABASE_KEY")
BUCKET_NAME = "exports"

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def upload_to_supabase(path: str, bucket_path: str):
    supabase = get_supabase_client()
    with open(path, "rb") as f:
        content = f.read()
    try:
        supabase.storage.from_(BUCKET_NAME).remove([bucket_path])
    except Exception as e:
        logging.warning(f"Suppression préalable échouée (ignorée): {e}")
    supabase.storage.from_(BUCKET_NAME).upload(bucket_path, content, {"content-type": "text/csv"})
    logging.info(f"✅ Fichier uploadé: {bucket_path}")
