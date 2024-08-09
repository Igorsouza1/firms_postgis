import os
from dotenv import load_dotenv

def load_env_variables() -> dict:
    load_dotenv()
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'schema_name': os.getenv('NOME_SCHEMA')
    }
