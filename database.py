import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Force absolute path
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")

print("DATABASE_URL LOADED:", DATABASE_URL)

def connect_db():
    return psycopg2.connect(DATABASE_URL)
