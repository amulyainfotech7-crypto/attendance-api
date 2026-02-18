import os
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
from pathlib import Path

# ======================================================
# LOAD ENV FILE (LOCAL SUPPORT)
# ======================================================

env_path = Path(__file__).parent / ".env"

if env_path.exists():
    load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")

# ======================================================
# DEBUG PRINT (SAFE)
# ======================================================

if DATABASE_URL:
    print("✅ DATABASE_URL loaded successfully")
else:
    print("❌ DATABASE_URL NOT FOUND")


# ======================================================
# DATABASE CONNECTION
# ======================================================

def connect_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    try:
        conn = psycopg2.connect(
            DATABASE_URL,
            sslmode="require",
            connect_timeout=10
        )
        return conn

    except OperationalError as e:
        print("❌ Database connection failed:", e)
        raise
