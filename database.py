import os
import time
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
# 🔥 FORCE FIX FOR RENDER URL FORMAT
if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgres://", 1)

# ======================================================
# DEBUG PRINT (SAFE)
# ======================================================

if DATABASE_URL:
    safe_url = DATABASE_URL.split("@")[-1]
    print("🌍 DATABASE_URL:", safe_url)
    print("✅ DATABASE_URL loaded successfully")
else:
    print("❌ DATABASE_URL NOT FOUND")



# ======================================================
# CONNECTION POOL
# ======================================================

DB_POOL = None


def init_db_pool(retries=5):
    """
    Initialize PostgreSQL connection pool (Render-safe).
    Includes retry + SSL fix + connection test.
    """

    global DB_POOL

    if DB_POOL is not None:
        return

    if not DATABASE_URL:
        raise Exception("❌ DATABASE_URL not set")

    db_url = DATABASE_URL.strip()

    # --------------------------------------------------
    # 🔥 FIX 1: Render URL compatibility
    # --------------------------------------------------
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    # --------------------------------------------------
    # 🔥 FIX 2: FORCE SSL IN URL (CRITICAL)
    # --------------------------------------------------
    if "sslmode" not in db_url:
        if "?" in db_url:
            db_url += "&sslmode=require"
        else:
            db_url += "?sslmode=require"

    safe_final = db_url.split("@")[-1]
    print("🔍 Final DB URL:", safe_final)

    # --------------------------------------------------
    # 🔁 RETRY LOOP (Render cold start fix)
    # --------------------------------------------------
    for attempt in range(retries):
        try:
            DB_POOL = SimpleConnectionPool(
                minconn=1,
                maxconn=20,
                dsn=db_url,

                connect_timeout=10,

                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,

                options='-c statement_timeout=30000'
            )

            # --------------------------------------------------
            # ✅ TEST CONNECTION (VERY IMPORTANT)
            # --------------------------------------------------
            conn = DB_POOL.getconn()
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.close()
            DB_POOL.putconn(conn)

            print("✅ PostgreSQL connection pool initialized & verified")
            return

        except Exception as e:
            print(f"⚠ DB Retry {attempt + 1}/{retries} failed:", e)
            time.sleep(3)

    raise Exception("❌ Could not connect to DB after retries")


# ======================================================
# GET CONNECTION
# ======================================================

def connect_db():
    global DB_POOL

    if DB_POOL is None:
        init_db_pool()

    try:
        conn = DB_POOL.getconn()

        # 🔥 Safety: ensure connection alive
        if conn.closed:
            print("⚠ Connection was closed. Reinitializing pool...")
            DB_POOL = None
            init_db_pool()
            conn = DB_POOL.getconn()

        return conn

    except Exception as e:
        print("❌ Failed to get DB connection:", e)

        # 🔥 HARD RECOVERY
        DB_POOL = None
        init_db_pool()

        return DB_POOL.getconn()


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


# ======================================================
# OPTIONAL: CLOSE ALL CONNECTIONS (SAFE SHUTDOWN)
# ======================================================

def close_all_connections():
    global DB_POOL

    try:
        if DB_POOL:
            DB_POOL.closeall()
            DB_POOL = None
            print("🔌 All DB connections closed")

    except Exception as e:
        print("⚠ Error closing DB pool:", e)