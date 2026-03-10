import os
import psycopg2
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
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
# CONNECTION POOL
# ======================================================

DB_POOL = None


def init_db_pool():
    """
    Initialize PostgreSQL connection pool.
    Called once when FastAPI server starts.
    """

    global DB_POOL

    if DB_POOL is None:

        try:
            DB_POOL = SimpleConnectionPool(
                minconn=1,
                maxconn=20,
                dsn=DATABASE_URL,
                sslmode="require",
                connect_timeout=10
            )

            print("🚀 PostgreSQL connection pool initialized")

        except OperationalError as e:
            print("❌ Failed to initialize DB pool:", e)
            raise


# ======================================================
# GET CONNECTION
# ======================================================

def connect_db():

    global DB_POOL

    if DB_POOL is None:
        init_db_pool()

    try:
        conn = DB_POOL.getconn()
        return conn

    except Exception as e:
        print("❌ Failed to get DB connection:", e)
        raise


# ======================================================
# RELEASE CONNECTION
# ======================================================

def release_db(conn):

    global DB_POOL

    try:
        if DB_POOL and conn:
            DB_POOL.putconn(conn)

    except Exception as e:
        print("⚠ Failed to release DB connection:", e)