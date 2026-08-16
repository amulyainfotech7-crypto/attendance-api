from fastapi import FastAPI, HTTPException, Body, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from database import connect_db, release_db, init_db_pool

from models import LoginModel, AttendanceRequest
from psycopg2.extras import execute_batch
from fastapi import Query
from typing import Optional

import hashlib
import base64
from datetime import datetime
import calendar
from datetime import date as dt_date


import threading
import requests
import time

app = FastAPI()

# ======================================================
# WEBSOCKET CLIENT REGISTRY
# ======================================================

connected_clients = []


# ======================================================
# REALTIME BROADCAST EVENT
# ======================================================

import asyncio

async def broadcast_event(table_name: str):

    disconnected = []

    for client in connected_clients:
        try:
            await client.send_json({
                "event": "table_updated",
                "table": table_name
            })
        except Exception:
            disconnected.append(client)

    for d in disconnected:
        if d in connected_clients:
            connected_clients.remove(d)

# ======================================================
# MIDDLEWARE
# ======================================================
# ======================================================
# AUTO DETECT SYNC TABLES
# ======================================================

def get_sync_tables():

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
        """)

        tables = [r[0] for r in cur.fetchall()]

        # tables we NEVER expose
        excluded = {
            "users",
            "pg_stat_statements"
        }

        return [t for t in tables if t not in excluded]

    except Exception as e:
        print("❌ get_sync_tables error:", e)
        return []

    finally:
        release_db(conn)


app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# HEALTH CHECK
# ======================================================

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "API running"}

@app.post("/sync-notify")
def sync_notify(data: dict):

    print("🔔 Sync completed notification received:", data)

    return {"status": "ok"}

# ======================================================
# AUTO SCHEMA MIGRATION (DESKTOP → CLOUD) — FINAL SAFE
# ======================================================

@app.post("/sync-schema")
def sync_schema(schema: dict = Body(...)):

    if not schema or not isinstance(schema, dict):
        raise HTTPException(status_code=400, detail="Invalid schema payload")

    conn = connect_db()
    cur = conn.cursor()

    created_tables = []
    added_columns = []

    try:

        for table_name, columns in schema.items():

            # --------------------------------------------------
            # Safety check for table name
            # --------------------------------------------------
            if not table_name or not isinstance(columns, dict):
                continue

            # --------------------------------------------------
            # Check if table exists
            # --------------------------------------------------
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_name=%s
                )
            """, (table_name,))

            table_exists = cur.fetchone()[0]

            # --------------------------------------------------
            # Map SQLite types → PostgreSQL
            # --------------------------------------------------
            def map_type(col_type):

                return {
                    "TEXT": "TEXT",
                    "INTEGER": "INTEGER",
                    "REAL": "DOUBLE PRECISION",
                    "BLOB": "BYTEA"
                }.get((col_type or "").upper(), "TEXT")

            # --------------------------------------------------
            # CREATE TABLE
            # --------------------------------------------------
            if not table_exists:

                col_defs = []

                for col, col_type in columns.items():

                    if not col:
                        continue

                    pg_type = map_type(col_type)

                    col_defs.append(f'"{col}" {pg_type}')

                if not col_defs:
                    continue

                create_sql = f'''
                CREATE TABLE "{table_name}" (
                    {",".join(col_defs)}
                );
                '''

                cur.execute(create_sql)

                created_tables.append(table_name)

                continue

            # --------------------------------------------------
            # EXISTING TABLE → CHECK COLUMNS
            # --------------------------------------------------
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public'
                AND table_name=%s
            """, (table_name,))

            existing_cols = {r[0] for r in cur.fetchall()}

            # --------------------------------------------------
            # ADD MISSING COLUMNS
            # --------------------------------------------------
            for col, col_type in columns.items():

                if not col or col in existing_cols:
                    continue

                pg_type = map_type(col_type)

                cur.execute(f'''
                    ALTER TABLE "{table_name}"
                    ADD COLUMN "{col}" {pg_type}
                ''')

                added_columns.append(f"{table_name}.{col}")

        conn.commit()

    except Exception as e:

        conn.rollback()
        release_db(conn)

        raise HTTPException(
            status_code=500,
            detail=f"Schema migration failed: {str(e)}"
        )

    release_db(conn)

    return {
        "status": "success",
        "tables_created": created_tables,
        "columns_added": added_columns
    }


from fastapi import Body

@app.post("/sync")
def sync_data(payload: dict = Body(...)):

    conn = connect_db()
    cur = conn.cursor()

    try:
        for table, rows in payload.items():

            print(f"🔥 SYNC TABLE: {table} ({len(rows)} rows)")

            for row in rows:

                print("➡ ROW:", row)

                # ===============================
                # ✅ RESULTS SEMESTER (FIXED)
                # ===============================
                if table == "results_semester":

                    cur.execute("""
                        INSERT INTO results_semester (
                            sbrn, semester, attempt,
                            total_marks, percentage, result_status,
                            sgpa, last_updated, version
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (sbrn, semester, attempt)
                        DO UPDATE SET
                            total_marks = EXCLUDED.total_marks,
                            percentage = EXCLUDED.percentage,
                            result_status = EXCLUDED.result_status,
                            sgpa = EXCLUDED.sgpa,
                            last_updated = EXCLUDED.last_updated,
                            version = EXCLUDED.version
                    """, (
                        row.get("sbrn"),
                        row.get("semester"),
                        row.get("attempt"),
                        row.get("total_marks"),
                        row.get("percentage"),
                        row.get("result_status"),
                        row.get("sgpa"),
                        row.get("last_updated"),
                        row.get("version", 1)
                    ))

                # ===============================
                # ✅ RESULT SUBJECTS (FIXED)
                # ===============================
                elif table == "result_subjects":

                    cur.execute("""
                        INSERT INTO result_subjects (
                            sbrn, semester, subject_id, attempt,
                            marks_obtained, max_marks,
                            grade, status,
                            last_updated, version
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (sbrn, semester, subject_id, attempt)
                        DO UPDATE SET
                            marks_obtained = EXCLUDED.marks_obtained,
                            max_marks = EXCLUDED.max_marks,
                            grade = EXCLUDED.grade,
                            status = EXCLUDED.status,
                            last_updated = EXCLUDED.last_updated,
                            version = EXCLUDED.version
                    """, (
                        row.get("sbrn"),
                        row.get("semester"),
                        row.get("subject_id"),
                        row.get("attempt"),
                        row.get("marks_obtained"),
                        row.get("max_marks"),
                        row.get("grade"),
                        row.get("status"),
                        row.get("last_updated"),
                        row.get("version", 1)
                    ))

                # ===============================
                # 🔁 OTHER TABLES (KEEP YOUR OLD LOGIC)
                # ===============================
                else:
                    # 👇 IMPORTANT: keep your existing insert/update logic here
                    # DO NOT leave it empty if other tables are syncing
                    pass

        conn.commit()
        print("✅ SYNC SUCCESS")

        return {
            "status": "success",
            "message": "Sync completed successfully"
        }

    except Exception as e:

        conn.rollback()
        print("❌ SYNC ERROR:", e)

        return {
            "status": "error",
            "message": str(e)
        }

    finally:
        release_db(conn)

# ======================================================
# REALTIME WEBSOCKET CHANNEL
# ======================================================

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):

    await ws.accept()
    connected_clients.append(ws)

    print("🔌 WebSocket client connected")

    try:
        while True:
            await ws.receive_text()

    except Exception:
        print("⚠ WebSocket client disconnected")

    finally:
        if ws in connected_clients:
            connected_clients.remove(ws)



# ======================================================
# KEEP RENDER SERVICE ALIVE (SMART VERSION)
# ======================================================

def keep_server_awake():

    import os

    base_url = os.getenv("RENDER_EXTERNAL_URL")

    if not base_url:
        print("⚠ No RENDER_EXTERNAL_URL found")
        return

    urls = [
        f"{base_url}/health",
        f"{base_url}/sync-all"
    ]

    while True:

        for url in urls:
            try:
                requests.get(url, timeout=10)
                print(f"💓 Keep-alive ping OK → {url}")
            except Exception as e:
                print(f"⚠ Keep-alive failed → {url} : {e}")

        time.sleep(600)


# ======================================================
# UNIVERSAL SYNC TABLE LIST
# ======================================================

SYNC_TABLES = {
    "students",
    "attendance_daily",
    "timetable_slots",
    "subjects",
    "semester_dates",
    "holidays",

    "faculty",
    "rooms",
    "departments",
    "faculty_subject_map",
    "activity_attendance"

    
}


# ======================================================
# PASSWORD VERIFY
# ======================================================

def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, salt_b64, hash_b64 = stored_hash.split("$")

        if algo != "pbkdf2_sha256":
            return False

        salt = base64.b64decode(salt_b64)
        original_hash = base64.b64decode(hash_b64)

        new_hash = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            120000
        )

        return new_hash == original_hash

    except Exception:
        return False

# ======================================================
# 🔥 WORKING DAY CHECK (CLOUD AUTHORITATIVE)
# ======================================================

def is_working_day(check_date: dt_date, department: str, semester: str):

    # ❌ Sunday
    if check_date.weekday() == 6:
        return False

    # ❌ 2nd Saturday
    if check_date.weekday() == 5:
        saturday_count = sum(
            1 for d in range(1, check_date.day + 1)
            if calendar.weekday(check_date.year, check_date.month, d) == 5
        )
        if saturday_count == 2:
            return False

    conn = connect_db()
    cur = conn.cursor()

    try:
        # ❌ Semester date range check
        cur.execute("""
            SELECT start_date, end_date
            FROM semester_dates
            WHERE LOWER(department)=LOWER(%s)
              AND LOWER(semester)=LOWER(%s)
        """, (department, semester))

        row = cur.fetchone()

        if row:
            if not (row[0] <= check_date <= row[1]):
                return False

        # ❌ Gazetted holiday check
        cur.execute(
            "SELECT 1 FROM holidays WHERE date=%s",
            (check_date,)
        )

        if cur.fetchone():
            return False

        return True

    except Exception as e:
        print("❌ is_working_day error:", e)
        return True  # fallback safe (don’t block system)

    finally:
        release_db(conn)




# ======================================================
# STARTUP – CREATE TABLES (FINAL PRODUCTION SAFE VERSION)
# ======================================================

@app.on_event("startup")
def startup():

    print("🚀 APP STARTING...")

    try:
        import os
        db_url = os.getenv("DATABASE_URL", "")
        print("🌍 DATABASE_URL:", db_url.split("@")[-1])

        # 🔥 STEP 1: Initialize DB pool
        init_db_pool()
        print("✅ DB Pool initialized")

        # 🔥 STEP 2: Test DB connection
        conn = connect_db()
        cur = conn.cursor()

        cur.execute("SELECT 1")
        print("✅ DB TEST SUCCESS")


        # ======================================================
        # USERS
        # ======================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users(
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                active INTEGER DEFAULT 1
            )
        """)

        # ======================================================
        # STUDENTS (SYNC SAFE + SESSION YEAR FIX)
        # ======================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS students(
                sbrn TEXT PRIMARY KEY,
                sync_id UUID,
                name TEXT,
                department TEXT,
                semester TEXT,
                section TEXT,
                session_year TEXT,

                mobile_no TEXT,
                father_name TEXT,
                district TEXT,
                photo TEXT,

                dob TEXT,
                address TEXT,
                state TEXT,
                pincode TEXT,
                gender TEXT,
                sr_no TEXT,

                course TEXT,
                batch TEXT,
                admission_date TEXT,
                year_semester TEXT,

                academic_status TEXT DEFAULT 'REGULAR',

                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version INTEGER DEFAULT 1,
                sync_pending INTEGER DEFAULT 0,
                is_deleted INTEGER DEFAULT 0,
                deleted_at TIMESTAMP
            )
        """)

        # ======================================================
        # SAFE COLUMN REPAIR
        # ======================================================

        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS sync_id UUID")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS session_year TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS mobile_no TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS father_name TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS district TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS photo TEXT")

        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS dob TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS address TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS state TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS pincode TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS gender TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS sr_no TEXT")

        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS course TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS batch TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS admission_date TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS year_semester TEXT")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS academic_status TEXT DEFAULT 'REGULAR'")

        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS sync_pending INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS is_deleted INTEGER DEFAULT 0")
        cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP")
        # 🔒 MANUAL DETENTION LOCK COLUMN (FINAL FIX)
        cur.execute("""
            ALTER TABLE students
            ADD COLUMN IF NOT EXISTS status_locked INTEGER DEFAULT 0
        """)

        # ======================================================
        # FACULTY TABLE (NEW)
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS faculty(
                faculty_id TEXT PRIMARY KEY,
                name TEXT,
                department TEXT,
                mobile TEXT,
                email TEXT,
                designation TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version INTEGER DEFAULT 1,
                is_deleted INTEGER DEFAULT 0
            )
        """)

        # ======================================================
        # SUBJECTS
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS subjects(
                subject_id TEXT,
                subject_name TEXT NOT NULL,
                department TEXT,
                semester TEXT,
                type TEXT,
                PRIMARY KEY (subject_id, semester, department)
            )
        """)

        # ======================================================
        # ROOMS
        # ======================================================

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rooms(
            room_id TEXT PRIMARY KEY,
            room_name TEXT,
            capacity INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # ======================================================
        # DEPARTMENTS
        # ======================================================

        cur.execute("""
        CREATE TABLE IF NOT EXISTS departments(
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # ======================================================
        # FACULTY SUBJECT MAP (FIXED PRIMARY KEY)
        # ======================================================

        # 🔥 TEMP FORCE RESET (ONLY FOR 1ST DEPLOY)
        cur.execute("DROP TABLE IF EXISTS faculty_subject_map CASCADE")

        cur.execute("""
        CREATE TABLE faculty_subject_map(
            faculty_id TEXT,
            subject_id TEXT,
            department TEXT,
            semester TEXT,
            section TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (faculty_id, subject_id, semester, department, section)
        )
        """)
        # ======================================================
        # TIMETABLE
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS timetable_slots(
                id SERIAL PRIMARY KEY,
                department TEXT NOT NULL,
                semester TEXT NOT NULL,
                section TEXT NOT NULL,
                day TEXT NOT NULL,
                period_no INTEGER NOT NULL,
                period_len INTEGER,
                type TEXT,
                subject_id TEXT,
                faculty_id TEXT,
                room TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version INTEGER DEFAULT 1,
                sync_pending INTEGER DEFAULT 0
            )
        """)

        # ======================================================
        # SEMESTER DATES
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS semester_dates(
                department TEXT,
                semester TEXT,
                start_date DATE,
                end_date DATE,
                PRIMARY KEY (department, semester)
            )
        """)

        # ======================================================
        # HOLIDAYS
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS holidays(
                date DATE PRIMARY KEY,
                description TEXT
            )
        """)

        # ======================================================
        # ATTENDANCE
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS attendance_daily(
                sbrn TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                subject TEXT NOT NULL,
                semester TEXT NOT NULL,
                section TEXT NOT NULL,
                class_date DATE NOT NULL,
                attended INTEGER NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (sbrn, subject_id, semester, section, class_date)
            );
        """)

        # ======================================================
        # ACTIVITY ATTENDANCE
        # LOCAL ↔ CLOUD SYNC READY
        # ======================================================

        cur.execute("""
            CREATE TABLE IF NOT EXISTS activity_attendance(
                id SERIAL PRIMARY KEY,

                sbrn TEXT NOT NULL,

                activity_type TEXT,
                activity_name TEXT,

                date DATE,

                weightage DOUBLE PRECISION DEFAULT 1,

                weight_theory DOUBLE PRECISION DEFAULT 0,
                weight_practical DOUBLE PRECISION DEFAULT 0,

                semester TEXT,
                section TEXT,

                session_year TEXT,
                department TEXT,

                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                version INTEGER DEFAULT 1,
                sync_pending INTEGER DEFAULT 0
            )
        """)

        # ======================================================
        # SAFE COLUMN MIGRATION
        # ======================================================

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS weight_theory
            DOUBLE PRECISION DEFAULT 0
        """)

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS weight_practical
            DOUBLE PRECISION DEFAULT 0
        """)

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS session_year
            TEXT
        """)

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS department
            TEXT
        """)

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS last_updated
            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS version
            INTEGER DEFAULT 1
        """)

        cur.execute("""
            ALTER TABLE activity_attendance
            ADD COLUMN IF NOT EXISTS sync_pending
            INTEGER DEFAULT 0
        """)

        # ======================================================
        # SAFE DEFAULT REPAIR
        # ======================================================

        cur.execute("""
            UPDATE activity_attendance
            SET version = 1
            WHERE version IS NULL
        """)

        cur.execute("""
            UPDATE activity_attendance
            SET sync_pending = 0
            WHERE sync_pending IS NULL
        """)

        cur.execute("""
            UPDATE activity_attendance
            SET last_updated = CURRENT_TIMESTAMP
            WHERE last_updated IS NULL
        """)

        # ======================================================
        # PERFORMANCE INDEX
        # ======================================================

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_activity_lookup
            ON activity_attendance (
                sbrn,
                semester,
                date
            )
        """)

        # ======================================================
        # ACTIVITY DATE INDEX
        # ======================================================

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_activity_date
            ON activity_attendance (
                activity_type,
                semester,
                session_year,
                date
            )
        """)

        # ======================================================
        # SYNC INDEX
        # ======================================================

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_activity_sync
            ON activity_attendance (
                sync_pending,
                last_updated
            )
        """)

        # ======================================================
        # CHECK FOR DUPLICATES
        # ======================================================
        #
        # IMPORTANT:
        # Do not automatically delete duplicate production data.
        # If duplicates exist, stop before creating the unique index.
        # ======================================================

        cur.execute("""
            SELECT
                sbrn,
                activity_type,
                semester,
                session_year,
                date,
                COUNT(*) AS duplicate_count
            FROM activity_attendance
            GROUP BY
                sbrn,
                activity_type,
                semester,
                session_year,
                date
            HAVING COUNT(*) > 1
            LIMIT 1
        """)

        duplicate_row = cur.fetchone()

        if duplicate_row:

            raise RuntimeError(
                "❌ Duplicate activity_attendance records detected. "
                "The logical unique index was not created. "
                "Please review the duplicate records before continuing."
            )

        # ======================================================
        # LOGICAL UNIQUE KEY
        # ======================================================
        #
        # Activity identity:
        #
        #   sbrn
        #   activity_type
        #   semester
        #   session_year
        #   date
        #
        # This logical identity is used for Local ↔ Cloud sync.
        # SQLite/PostgreSQL auto-increment IDs are NOT used
        # as the cross-database identity.
        #
        # ======================================================

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_activity_attendance_sync_key
            ON activity_attendance (
                sbrn,
                activity_type,
                semester,
                session_year,
                date
            )
        """)

        # ======================================================
        # VERIFY ACTIVITY ATTENDANCE SCHEMA
        # ======================================================

        cur.execute("""
            SELECT
                column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'activity_attendance'
            ORDER BY ordinal_position
        """)

        activity_columns = [
            row[0]
            for row in cur.fetchall()
        ]

        required_activity_columns = [
            "id",
            "sbrn",
            "activity_type",
            "activity_name",
            "date",
            "weightage",
            "weight_theory",
            "weight_practical",
            "semester",
            "section",
            "session_year",
            "department",
            "last_updated",
            "version",
            "sync_pending",
        ]

        missing_activity_columns = [
            column
            for column in required_activity_columns
            if column not in activity_columns
        ]

        if missing_activity_columns:

            raise RuntimeError(
                "❌ activity_attendance schema incomplete. "
                f"Missing columns: {missing_activity_columns}"
            )

        print(
            "✅ activity_attendance schema verified"
        )

        print(
            "   Columns:",
            ", ".join(activity_columns)
        )

        # ======================================================
        # VERIFY LOGICAL UNIQUE INDEX
        # ======================================================

        cur.execute("""
            SELECT
                indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename = 'activity_attendance'
            AND indexname = 'uq_activity_attendance_sync_key'
        """)

        if cur.fetchone():

            print(
                "✅ activity_attendance sync unique index verified"
            )

        else:

            raise RuntimeError(
                "❌ activity_attendance sync unique index "
                "was not created."
            )


        # ======================================================
        # PERFORMANCE INDEXES
        # ======================================================

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_attendance_semester
            ON attendance_daily (semester);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_attendance_subject_id
            ON attendance_daily (subject_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_attendance_last_updated
            ON attendance_daily (last_updated);
        """)


        # ======================================================
        # COMMIT + RELEASE
        # ======================================================

        conn.commit()
        release_db(conn)

        print(
            "✅ PostgreSQL Server Ready (SYNC ENABLED)"
        )

        threading.Thread(
            target=keep_server_awake,
            daemon=True
        ).start()

            
    except Exception as e:
        import traceback
        print("❌ STARTUP FAILED:")
        traceback.print_exc()
        raise e

# ======================================================
# LOGIN
# ======================================================

@app.post("/login")
def login(data: LoginModel):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT username, password, role, active
        FROM users
        WHERE username=%s
    """, (data.username,))

    user = cur.fetchone()
    release_db(conn)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid Username")

    if user[3] == 0:
        raise HTTPException(status_code=403, detail="Account Disabled")

    if not verify_password(data.password, user[1]):
        raise HTTPException(status_code=401, detail="Invalid Password")

    return {
        "status": "success",
        "username": user[0],
        "role": user[2]
    }

# ======================================================
# GET DEPARTMENTS
# ======================================================

@app.get("/departments")
def get_departments():

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT department
            FROM departments
            WHERE department IS NOT NULL
              AND TRIM(department) <> ''
            ORDER BY department
        """)

        rows = cur.fetchall()

        return [
            str(row[0]).strip()
            for row in rows
            if row[0]
        ]

    except Exception as e:
        print("❌ Failed to load departments:", e)

        raise HTTPException(
            status_code=500,
            detail=f"Failed to load departments: {str(e)}"
        )

    finally:
        release_db(conn)

# ======================================================
# GET SEMESTERS
# ======================================================

@app.get("/semesters")
def get_semesters(department: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT semester
        FROM students
        WHERE LOWER(department)=LOWER(%s)
        ORDER BY semester
    """, (department,))

    rows = cur.fetchall()
    release_db(conn)

    return [r[0] for r in rows]


# ======================================================
# 🔥 FACULTY SYNC (LOCAL → CLOUD)
# ======================================================

@app.post("/sync/faculty")
def sync_faculty(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO faculty
    (faculty_id,name,department,mobile,email,designation,last_updated,version,is_deleted)
    VALUES
    (%(faculty_id)s,%(name)s,%(department)s,%(mobile)s,%(email)s,%(designation)s,%(last_updated)s,%(version)s,%(is_deleted)s)
    ON CONFLICT (faculty_id)
    DO UPDATE SET
        name = EXCLUDED.name,
        department = EXCLUDED.department,
        mobile = EXCLUDED.mobile,
        email = EXCLUDED.email,
        designation = EXCLUDED.designation,
        last_updated = EXCLUDED.last_updated,
        version = EXCLUDED.version,
        is_deleted = EXCLUDED.is_deleted
    WHERE faculty.version <= EXCLUDED.version;
    """

    execute_batch(cur, query, records)

    conn.commit()
    release_db(conn)

    return {"status":"success","rows":len(records)}


# ======================================================
# 🔥 FACULTY CLOUD → LOCAL
# ======================================================
@app.get("/sync/faculty")
def sync_faculty_from_cloud(since: Optional[str] = None):

    conn = connect_db()
    cur = conn.cursor()

    if since:
        cur.execute("""
            SELECT * FROM faculty
            WHERE last_updated > %s
            ORDER BY last_updated ASC
        """,(since,))
    else:
        cur.execute("SELECT * FROM faculty ORDER BY last_updated ASC")

    rows = cur.fetchall()

    columns = [d[0] for d in cur.description]

    records = []

    for r in rows:

        rec = dict(zip(columns,r))

        for k,v in rec.items():
            if hasattr(v,"isoformat"):
                rec[k] = v.isoformat()

        records.append(rec)

    release_db(conn)

    return {
        "status":"success",
        "records":records,
        "count":len(records)
    }

# ======================================================
# 🔥 SYNC TIMETABLE (LOCAL → CLOUD) – FINAL SAFE VERSION
# ======================================================

@app.post("/sync/timetable")
def sync_timetable(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    conn = connect_db()
    cur = conn.cursor()

    try:

        # --------------------------------------------------
        # Ensure table exists (fresh cloud safety)
        # --------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS timetable_slots(
            id SERIAL PRIMARY KEY,
            department TEXT NOT NULL,
            semester TEXT NOT NULL,
            section TEXT NOT NULL,
            day TEXT NOT NULL,
            period_no INTEGER NOT NULL,
            period_len INTEGER,
            type TEXT,
            subject_id TEXT,
            faculty_id TEXT,
            room TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            version INTEGER DEFAULT 1,
            sync_pending INTEGER DEFAULT 0,
            UNIQUE(department,semester,section,day,period_no)
        )
        """)

        # --------------------------------------------------
        # Normalize records
        # --------------------------------------------------

        normalized = []

        for r in records:

            normalized.append({
                "department": r.get("department"),
                "semester": r.get("semester"),
                "section": r.get("section"),
                "day": r.get("day"),
                "period_no": r.get("period_no"),
                "period_len": r.get("period_len"),
                "type": r.get("type"),
                "subject_id": r.get("subject_id"),
                "faculty_id": r.get("faculty_id"),
                "room": r.get("room"),
                "last_updated": r.get("last_updated") or datetime.utcnow(),
                "version": r.get("version",1)
            })

        # --------------------------------------------------
        # UPSERT TIMETABLE
        # --------------------------------------------------

        query = """
        INSERT INTO timetable_slots
        (department,semester,section,day,period_no,
         period_len,type,subject_id,faculty_id,room,
         last_updated,version)

        VALUES
        (%(department)s,%(semester)s,%(section)s,%(day)s,%(period_no)s,
         %(period_len)s,%(type)s,%(subject_id)s,%(faculty_id)s,%(room)s,
         %(last_updated)s,%(version)s)

        ON CONFLICT (department,semester,section,day,period_no)
        DO UPDATE SET
            period_len = EXCLUDED.period_len,
            type = EXCLUDED.type,
            subject_id = EXCLUDED.subject_id,
            faculty_id = EXCLUDED.faculty_id,
            room = EXCLUDED.room,
            last_updated = EXCLUDED.last_updated,
            version = EXCLUDED.version
        WHERE timetable_slots.version <= EXCLUDED.version;
        """

        execute_batch(cur, query, normalized)

        conn.commit()

        # --------------------------------------------------
        # 🔥 AUTO CREATE SUBJECTS FROM TIMETABLE
        # --------------------------------------------------

        cur.execute("""
        INSERT INTO subjects (subject_id,subject_name,department,semester,type)
        SELECT DISTINCT
            subject_id,
            subject_id,
            department,
            semester,
            type
        FROM timetable_slots
        WHERE subject_id IS NOT NULL
        ON CONFLICT DO NOTHING
        """)

        conn.commit()

        # --------------------------------------------------
        # Broadcast realtime update
        # --------------------------------------------------

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcast_event("timetable_slots"))
        except RuntimeError:
            pass

    except Exception as e:

        conn.rollback()
        release_db(conn)

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    release_db(conn)

    return {
        "status":"success",
        "rows_processed":len(normalized)
    }

# ======================================================
# 🔥 CLOUD → DESKTOP TIMETABLE SYNC (INCREMENTAL SAFE)
# ======================================================

@app.get("/sync/timetable")
def get_timetable_sync(last_sync: Optional[str] = Query(default=None)):

    conn = connect_db()
    cur = conn.cursor()

    try:

        if last_sync:
            try:
                parsed_sync = datetime.fromisoformat(last_sync)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid 'last_sync' timestamp format."
                )

            cur.execute("""
                SELECT department, semester, section, day,
                       period_no, period_len, type,
                       subject_id, faculty_id, room,
                       last_updated, version
                FROM timetable_slots
                WHERE last_updated > %s
                ORDER BY last_updated ASC
            """, (parsed_sync,))
        else:
            cur.execute("""
                SELECT department, semester, section, day,
                       period_no, period_len, type,
                       subject_id, faculty_id, room,
                       last_updated, version
                FROM timetable_slots
                ORDER BY last_updated ASC
            """)

        rows = cur.fetchall()

    except Exception as e:
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    data = [
        {
            "department": r[0],
            "semester": r[1],
            "section": r[2],
            "day": r[3],
            "period_no": r[4],
            "period_len": r[5],
            "type": r[6],
            "subject_id": r[7],
            "faculty_id": r[8],
            "room": r[9],
            "last_updated": r[10].isoformat() if r[10] else None,
            "version": r[11]
        }
        for r in rows
    ]

    latest_sync = None
    if rows:
        latest_sync = rows[-1][10].isoformat()

    return {
        "status": "success",
        "count": len(data),
        "latest_sync": latest_sync,
        "records": data
    }

# ======================================================
# GET TIMETABLE
# ======================================================

@app.get("/timetable")
def get_timetable(department: str, semester: str, day: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT section, period_no, subject_id, faculty_id, room
        FROM timetable_slots
        WHERE LOWER(department)=LOWER(%s)
          AND LOWER(semester)=LOWER(%s)
          AND LOWER(day)=LOWER(%s)
        ORDER BY period_no
    """, (department, semester, day))

    rows = cur.fetchall()
    release_db(conn)

    return [
        {
            "section": r[0],
            "period_no": r[1],
            "subject_id": r[2],
            "faculty_id": r[3],
            "room": r[4]
        }
        for r in rows
    ]


# ======================================================
# SUBJECTS BY DATE (FIXED WITH SECTION SUPPORT)
# ======================================================

@app.get("/subjects-by-date")
def get_subjects_by_date(
    department: str,
    semester: str,
    date: str
):
    """
    Return timetable subjects with the REAL full subject name.

    timetable_slots:
        subject_id = source for scheduled subject

    subjects:
        subject_name/type = source for subject details

    subject_semester_map:
        semester + department = source for subject mapping

    Placeholder subject rows where:
        subject_name == subject_id
    are ignored.
    """

    try:
        parsed_date = datetime.strptime(
            date,
            "%Y-%m-%d"
        ).date()

        if not is_working_day(
            parsed_date,
            department,
            semester
        ):
            print(
                "DEBUG: Holiday or Non-working day → No subjects"
            )
            return []

        weekday_short = parsed_date.strftime("%a").strip()

    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format"
        )

    conn = connect_db()
    cur = conn.cursor()

    try:

        cur.execute("""
            SELECT
                t.subject_id,

                COALESCE(
                    NULLIF(TRIM(s.subject_name), ''),
                    t.subject_id
                ) AS subject_name,

                COALESCE(
                    NULLIF(TRIM(s.type), ''),
                    t.type
                ) AS type,

                STRING_AGG(
                    DISTINCT t.section,
                    ','
                ) AS sections,

                MIN(t.period_no) AS first_period

            FROM timetable_slots t

            LEFT JOIN LATERAL (

                SELECT
                    sx.subject_name,
                    sx.type

                FROM subjects sx

                INNER JOIN subject_semester_map sm
                    ON LOWER(TRIM(sm.subject_id))
                       =
                       LOWER(TRIM(sx.subject_id))

                WHERE
                    LOWER(TRIM(sx.subject_id))
                        =
                    LOWER(TRIM(t.subject_id))

                    AND

                    LOWER(TRIM(sm.semester))
                        =
                    LOWER(TRIM(t.semester))

                    AND

                    (
                        LOWER(TRIM(COALESCE(sm.department, '')))
                            =
                        LOWER(TRIM(t.department))

                        OR

                        COALESCE(TRIM(sm.department), '') = ''
                    )

                    -- Ignore placeholder:
                    -- AS -> AS
                    AND COALESCE(
                        TRIM(sx.subject_name),
                        ''
                    ) <> ''

                    AND LOWER(
                        TRIM(sx.subject_name)
                    )
                    <>
                    LOWER(
                        TRIM(sx.subject_id)
                    )

                ORDER BY

                    -- Exact department mapping first
                    CASE
                        WHEN LOWER(
                            TRIM(COALESCE(sm.department, ''))
                        )
                        =
                        LOWER(
                            TRIM(t.department)
                        )
                        THEN 0

                        -- COMMON / blank department second
                        ELSE 1
                    END

                LIMIT 1

            ) s ON TRUE

            WHERE
                LOWER(TRIM(t.department))
                    =
                LOWER(TRIM(%s))

                AND

                LOWER(TRIM(t.semester))
                    =
                LOWER(TRIM(%s))

                AND

                LOWER(TRIM(t.day))
                    =
                LOWER(TRIM(%s))

            GROUP BY
                t.subject_id,
                s.subject_name,
                s.type,
                t.type

            ORDER BY
                first_period

        """, (
            department,
            semester,
            weekday_short
        ))

        rows = cur.fetchall()

        print(
            "DEBUG subjects found:",
            len(rows)
        )

        result = []

        for r in rows:

            subject_id = str(
                r[0] or ""
            ).strip()

            subject_name = str(
                r[1] or ""
            ).strip()

            subject_type = str(
                r[2] or ""
            ).strip()

            sections = (
                r[3].split(",")
                if r[3]
                else []
            )

            print(
                "📚 SUBJECT API → "
                f"ID={subject_id} | "
                f"NAME={subject_name} | "
                f"TYPE={subject_type}"
            )

            result.append({
                "subject_id": subject_id,
                "subject_name": subject_name,
                "type": subject_type,
                "sections": [
                    s.strip()
                    for s in sections
                    if s.strip()
                ]
            })

        return result

    finally:
        release_db(conn)

# ======================================================
# GET STUDENTS (SYNC SAFE VERSION - FINAL FIXED)
# ======================================================

@app.get("/students")
def get_students(department: str, semester: str, section: str):

    conn = connect_db()
    cur = conn.cursor()

    try:

        # ======================================================
        # 🔥 THEORY CASE → section = all → ignore section filter
        # ======================================================
        if section.lower() == "all":

            cur.execute("""
                SELECT
                    sbrn,
                    name,
                    department,
                    semester,
                    section
                FROM students
                WHERE LOWER(COALESCE(department,'')) = LOWER(%s)
                  AND LOWER(COALESCE(semester,''))   = LOWER(%s)

                  -- 🔥 REMOVE ONLY MANUAL DETAINED STUDENTS
                  AND COALESCE(status_locked,0) = 0
                  AND UPPER(COALESCE(academic_status,'ACTIVE')) = 'ACTIVE'
                ORDER BY sbrn
            """, (department, semester))

        # ======================================================
        # 🔥 PRACTICAL CASE → filter by section
        # ======================================================
        else:

            cur.execute("""
                SELECT
                    sbrn,
                    name,
                    department,
                    semester,
                    section
                FROM students
                WHERE LOWER(COALESCE(department,'')) = LOWER(%s)
                  AND LOWER(COALESCE(semester,''))   = LOWER(%s)
                  AND LOWER(COALESCE(section,''))    = LOWER(%s)

                  -- 🔥 REMOVE ONLY MANUAL DETAINED STUDENTS
                  AND COALESCE(status_locked,0) = 0
                  AND UPPER(COALESCE(academic_status,'ACTIVE')) = 'ACTIVE'

                ORDER BY sbrn
            """, (department, semester, section))

        rows = cur.fetchall()

    except Exception as e:
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    # ======================================================
    # RESPONSE FORMAT
    # ======================================================
    return [
        {
            "sbrn": r[0],
            "name": r[1],
            "department": r[2],
            "semester": r[3],
            "section": r[4]
        }
        for r in rows
    ]


from fastapi import Body, HTTPException

# ======================================================
# 🔥 DELETE STUDENT (CLOUD SIDE)
# ======================================================

@app.delete("/delete/students/{sbrn}")
def delete_student(sbrn: str):

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM students
            WHERE sbrn = %s
        """, (sbrn,))

        conn.commit()

        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Student not found")

        return {
            "status": "deleted",
            "sbrn": sbrn
        }

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        release_db(conn)

# ======================================================
# 🔥 GENERIC CLOUD DELETE
# ======================================================

@app.delete("/delete/{table}/{row_id}")
def delete_cloud_row(table: str, row_id: str):

    conn = connect_db()
    cur = conn.cursor()

    # --------------------------------------------------
    # SECURITY: ONLY ALLOW REAL APPLICATION TABLES
    # --------------------------------------------------

    allowed_tables = {
        "holidays",
        "rooms",
        "students",
        "subjects",
        "faculty",
        "faculty_subject_map",
        "subject_semester_map",
        "timetable_slots",
        "attendance_daily",
        "activity_attendance",
        "results_semester",
        "result_subjects",
        "exam_marks",
        "master_attendance",
    }

    if table not in allowed_tables:
        release_db(conn)

        raise HTTPException(
            status_code=403,
            detail=f"Table '{table}' is not allowed for DELETE."
        )

    try:

        # --------------------------------------------------
        # HOLIDAYS
        # --------------------------------------------------
        if table == "holidays":

            cur.execute(
                """
                DELETE FROM holidays
                WHERE date = %s
                """,
                (row_id,)
            )

        # --------------------------------------------------
        # STUDENTS
        # --------------------------------------------------
        elif table == "students":

            cur.execute(
                """
                DELETE FROM students
                WHERE sbrn = %s
                """,
                (row_id,)
            )

        # --------------------------------------------------
        # ROOMS
        # --------------------------------------------------
        elif table == "rooms":

            cur.execute(
                """
                DELETE FROM rooms
                WHERE room_id = %s
                """,
                (row_id,)
            )

        # --------------------------------------------------
        # SUBJECTS
        # --------------------------------------------------
        elif table == "subjects":

            cur.execute(
                """
                DELETE FROM subjects
                WHERE subject_id = %s
                """,
                (row_id,)
            )

        # --------------------------------------------------
        # FACULTY
        # --------------------------------------------------
        elif table == "faculty":

            cur.execute(
                """
                DELETE FROM faculty
                WHERE faculty_id = %s
                """,
                (row_id,)
            )

        # --------------------------------------------------
        # GENERIC FALLBACK
        # --------------------------------------------------
        else:

            # For tables not having a special key above,
            # use the standard "id" primary key.
            cur.execute(
                f"""
                DELETE FROM "{table}"
                WHERE id = %s
                """,
                (row_id,)
            )

        deleted_rows = cur.rowcount

        conn.commit()

        # --------------------------------------------------
        # IDEMPOTENT DELETE
        # --------------------------------------------------
        #
        # If the row is already gone, that is still a
        # successful final state.
        #
        # This prevents permanent retry loops.
        # --------------------------------------------------

        print(
            f"🗑 CLOUD DELETE → "
            f"{table}:{row_id} "
            f"(rows={deleted_rows})"
        )

        return {
            "status": "deleted",
            "table": table,
            "row_id": row_id,
            "rows_deleted": deleted_rows
        }

    except Exception as e:

        conn.rollback()

        print(
            f"❌ CLOUD DELETE FAILED → "
            f"{table}:{row_id}: {e}"
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        release_db(conn)
# ======================================================
# CHECK ATTENDANCE EXISTS (FIXED)
# ======================================================

@app.get("/attendance-exists")
def attendance_exists(semester: str, section: str, subject: str, date: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM attendance_daily
        WHERE LOWER(semester)=LOWER(%s)
          AND LOWER(subject)=LOWER(%s)
          AND class_date=%s
          AND LOWER(section)=LOWER(%s)
        LIMIT 1
    """, (semester, subject, date, section))

    exists = cur.fetchone() is not None
    release_db(conn)

    return {"exists": exists}

# ======================================================
# MARK ATTENDANCE (PERMANENT DESKTOP-ALIGNED VERSION)
# ======================================================

@app.post("/mark-attendance")
def mark_attendance(data: AttendanceRequest):

    conn = connect_db()
    cur = conn.cursor()

    try:
        # --------------------------------------------------
        # 1️⃣ Validate date
        # --------------------------------------------------
        class_date = datetime.strptime(data.date, "%Y-%m-%d").date()

        # 🔥 BLOCK ATTENDANCE ON HOLIDAYS
        if not is_working_day(class_date, data.department, data.semester):
            return {
                "status": "holiday",
                "message": "Attendance cannot be marked on holidays"
            }

        day_short = class_date.strftime("%a")

        section_value = (data.section or "").lower()

        # --------------------------------------------------
        # 2️⃣ Verify timetable period exists
        # --------------------------------------------------
        if section_value == "all":
            # THEORY
            cur.execute("""
                SELECT 1
                FROM timetable_slots
                WHERE LOWER(TRIM(department)) = LOWER(TRIM(%s))
                  AND LOWER(TRIM(semester))   = LOWER(TRIM(%s))
                  AND LOWER(TRIM(subject_id)) = LOWER(TRIM(%s))
                  AND LOWER(TRIM(day))        = LOWER(TRIM(%s))
                LIMIT 1
            """, (
                data.department,
                data.semester,
                data.subject,
                day_short
            ))
        else:
            # PRACTICAL
            cur.execute("""
                SELECT 1
                FROM timetable_slots
                WHERE LOWER(TRIM(department)) = LOWER(TRIM(%s))
                  AND LOWER(TRIM(semester))   = LOWER(TRIM(%s))
                  AND LOWER(TRIM(section))    = LOWER(TRIM(%s))
                  AND LOWER(TRIM(subject_id)) = LOWER(TRIM(%s))
                  AND LOWER(TRIM(day))        = LOWER(TRIM(%s))
                LIMIT 1
            """, (
                data.department,
                data.semester,
                data.section,
                data.subject,
                day_short
            ))

        if not cur.fetchone():
            return {
                "status": "no_period",
                "message": "No period today"
            }

        # --------------------------------------------------
        # 3️⃣ Save attendance (COMPOSITE PRIMARY KEY SAFE)
        # --------------------------------------------------
        for rec in data.attendance:

            cur.execute("""
                INSERT INTO attendance_daily
                (sbrn, subject_id, subject, semester, section, class_date, attended)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sbrn, subject_id, semester, section, class_date)
                DO UPDATE SET
                    attended = EXCLUDED.attended,
                    last_updated = EXCLUDED.last_updated
                WHERE attendance_daily.last_updated < EXCLUDED.last_updated
            """, (
                rec.sbrn,
                data.subject,   # subject_id
                data.subject,   # subject (for readability)
                data.semester,
                data.section,
                data.date,
                1 if rec.present else 0
            ))

        conn.commit()

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcast_event("attendance_daily"))
        except RuntimeError:
            pass

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        release_db(conn)

    return {"status": "saved"}

# ======================================================
# GET ATTENDANCE (ALIGNED WITH NEW STRUCTURE)
# ======================================================

@app.get("/attendance")
def get_attendance(department: str, semester: str, month: int, year: int, subject: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT a.sbrn,
               a.subject,
               a.semester,
               a.section,
               a.class_date,
               a.attended
        FROM attendance_daily a
        JOIN students s ON a.sbrn = s.sbrn
        WHERE LOWER(s.department)=LOWER(%s)
          AND LOWER(a.semester)=LOWER(%s)
          AND EXTRACT(MONTH FROM a.class_date)=%s
          AND EXTRACT(YEAR FROM a.class_date)=%s
          AND LOWER(a.subject)=LOWER(%s)
    """, (department, semester, month, year, subject))

    rows = cur.fetchall()
    release_db(conn)

    return [
        {
            "sbrn": r[0],
            "subject": r[1],   # 🔥 changed from subject_id
            "semester": r[2],
            "section": r[3],
            "class_date": r[4].strftime("%Y-%m-%d"),
            "attended": r[5]
        }
        for r in rows
    ]



# ======================================================
# 🔥 SYNC STUDENTS (LOCAL → CLOUD) — FINAL FIXED
# ======================================================

@app.post("/sync/students")
def sync_students(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    from datetime import datetime, timezone

    # ======================================================
    # SAFE TIMESTAMP NORMALIZER
    # ======================================================

    def normalize_timestamp(value, field_name, sbrn=None):

        # NULL
        if value is None:
            return None

        # Empty string / NULL-like values
        if isinstance(value, str):

            value = value.strip()

            if value == "":
                return None

            if value.upper() in (
                "NULL",
                "NONE",
                "NAN",
            ):
                return None

        # Already datetime
        if isinstance(value, datetime):
            return value

        # Convert string
        try:

            value = str(value).strip()

            # ISO format
            if "T" in value:

                value = value.replace("Z", "+00:00")

                return datetime.fromisoformat(value)

            # Normal PostgreSQL/SQLite formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y %H:%M",
                "%d-%m-%Y",
            ]

            for fmt in formats:

                try:
                    return datetime.strptime(value, fmt)

                except ValueError:
                    continue

        except Exception:
            pass

        print(
            f"⚠ Invalid {field_name} "
            f"for SBRN {sbrn}: {value!r} → NULL"
        )

        return None

    # ======================================================
    # NORMALIZE ALL STUDENTS
    # ======================================================

    normalized = []

    for r in records:

        if not isinstance(r, dict):
            continue

        # --------------------------------------------------
        # SBRN
        # --------------------------------------------------

        sbrn = r.get("sbrn")

        if sbrn is None:
            continue

        sbrn = str(sbrn).strip()

        if not sbrn:
            continue

        # --------------------------------------------------
        # VERSION
        # --------------------------------------------------

        try:
            version = int(r.get("version") or 1)

        except Exception:
            version = 1

        # --------------------------------------------------
        # SESSION YEAR
        # --------------------------------------------------

        session_year = r.get("session_year")

        if (
            session_year is None
            or str(session_year).strip() == ""
        ):

            try:

                if len(sbrn) >= 2:

                    year_prefix = int(sbrn[:2])

                    session_year = str(
                        2000 + year_prefix
                    )

            except Exception:

                session_year = None

        # --------------------------------------------------
        # ACADEMIC STATUS
        # --------------------------------------------------

        academic_status = r.get(
            "academic_status"
        )

        if (
            academic_status is None
            or str(academic_status).strip() == ""
        ):

            academic_status = "ACTIVE"

        else:

            academic_status = (
                str(academic_status)
                .strip()
                .upper()
                .replace(" ", "_")
            )

        # --------------------------------------------------
        # SAFE INTEGER FIELDS
        # --------------------------------------------------

        try:
            is_deleted = int(
                r.get("is_deleted") or 0
            )
        except Exception:
            is_deleted = 0

        try:
            status_locked = int(
                r.get("status_locked") or 0
            )
        except Exception:
            status_locked = 0

        try:
            status_priority = int(
                r.get("status_priority") or 0
            )
        except Exception:
            status_priority = 0

        # --------------------------------------------------
        # TIMESTAMP FIELDS
        # --------------------------------------------------

        last_updated = normalize_timestamp(
            r.get("last_updated"),
            "last_updated",
            sbrn
        )

        deleted_at = normalize_timestamp(
            r.get("deleted_at"),
            "deleted_at",
            sbrn
        )

        # last_updated MUST have a value
        if last_updated is None:
            last_updated = datetime.now()

        # --------------------------------------------------
        # BUILD RECORD
        # --------------------------------------------------

        normalized.append({

            "sbrn": sbrn,

            "sync_id": r.get("sync_id"),

            "name": r.get("name"),

            "semester": r.get("semester"),

            "section": r.get("section"),

            "department": r.get("department"),

            "session_year": session_year,

            "mobile_no": r.get("mobile_no"),

            "father_name": r.get("father_name"),

            "district": r.get("district"),

            "photo": r.get("photo"),

            "dob": r.get("dob"),

            "address": r.get("address"),

            "state": r.get("state"),

            "pincode": r.get("pincode"),

            "gender": r.get("gender"),

            "sr_no": r.get("sr_no"),

            "course": r.get("course"),

            "batch": r.get("batch"),

            "admission_date": r.get("admission_date"),

            "year_semester": r.get("year_semester"),

            "academic_status": academic_status,

            "last_updated": last_updated,

            "version": version,

            "is_deleted": is_deleted,

            "deleted_at": deleted_at,

            "status_locked": status_locked,

            "status_priority": status_priority,
        })

    # ======================================================
    # VALIDATION
    # ======================================================

    if not normalized:

        return {
            "status": "no_valid_records"
        }

    print(
        f"👨‍🎓 Student records normalized: "
        f"{len(normalized)}"
    )

    # ======================================================
    # DATABASE
    # ======================================================

    conn = connect_db()
    cur = conn.cursor()

    try:

        # ==================================================
        # EXISTING VERSIONS
        # ==================================================

        sbrns = list({
            r["sbrn"]
            for r in normalized
        })

        placeholders = ",".join(
            ["%s"] * len(sbrns)
        )

        cur.execute(
            f"""
            SELECT
                sbrn,
                version
            FROM students
            WHERE sbrn IN ({placeholders})
            """,
            sbrns
        )

        existing_versions = {
            row[0]: (row[1] or 0)
            for row in cur.fetchall()
        }

        # ==================================================
        # FILTER RECORDS
        # ==================================================

        filtered = []

        for r in normalized:

            cloud_version = (
                existing_versions.get(
                    r["sbrn"],
                    0
                )
                or 0
            )

            local_version = (
                r.get("version")
                or 1
            )

            try:

                # Allow equal version.
                # This is important for CSV re-import
                # where section/name/etc. changed.

                if int(local_version) >= int(
                    cloud_version
                ):

                    filtered.append(r)

            except Exception:

                filtered.append(r)

        if not filtered:

            release_db(conn)

            print(
                f"⚡ Students skipped: "
                f"{len(normalized)}"
            )

            return {
                "status": "up_to_date"
            }

        print(
            f"📦 Students to UPSERT: "
            f"{len(filtered)}"
        )

        # ==================================================
        # UPSERT
        # ==================================================

        query = """

        INSERT INTO students
        (
            sbrn,
            sync_id,
            name,
            semester,
            section,
            department,

            session_year,
            mobile_no,
            father_name,
            district,
            photo,

            dob,
            address,
            state,
            pincode,
            gender,
            sr_no,

            course,
            batch,
            admission_date,
            year_semester,
            academic_status,

            last_updated,
            version,
            is_deleted,
            deleted_at,

            status_locked,
            status_priority
        )

        VALUES
        (
            %(sbrn)s,
            %(sync_id)s,
            %(name)s,
            %(semester)s,
            %(section)s,
            %(department)s,

            %(session_year)s,
            %(mobile_no)s,
            %(father_name)s,
            %(district)s,
            %(photo)s,

            %(dob)s,
            %(address)s,
            %(state)s,
            %(pincode)s,
            %(gender)s,
            %(sr_no)s,

            %(course)s,
            %(batch)s,
            %(admission_date)s,
            %(year_semester)s,
            %(academic_status)s,

            %(last_updated)s,
            %(version)s,
            %(is_deleted)s,
            %(deleted_at)s,

            %(status_locked)s,
            %(status_priority)s
        )

        ON CONFLICT (sbrn)

        DO UPDATE SET

            sync_id =
                EXCLUDED.sync_id,

            name =
                EXCLUDED.name,

            semester =
                EXCLUDED.semester,

            section =
                EXCLUDED.section,

            department =
                EXCLUDED.department,

            session_year =
                EXCLUDED.session_year,

            mobile_no =
                EXCLUDED.mobile_no,

            father_name =
                EXCLUDED.father_name,

            district =
                EXCLUDED.district,

            photo =
                EXCLUDED.photo,

            dob =
                EXCLUDED.dob,

            address =
                EXCLUDED.address,

            state =
                EXCLUDED.state,

            pincode =
                EXCLUDED.pincode,

            gender =
                EXCLUDED.gender,

            sr_no =
                EXCLUDED.sr_no,

            course =
                EXCLUDED.course,

            batch =
                EXCLUDED.batch,

            admission_date =
                EXCLUDED.admission_date,

            year_semester =
                EXCLUDED.year_semester,

            academic_status =
                CASE
                    WHEN students.status_locked = 1
                    THEN students.academic_status
                    ELSE EXCLUDED.academic_status
                END,

            last_updated =
                EXCLUDED.last_updated,

            version =
                EXCLUDED.version,

            is_deleted =
                EXCLUDED.is_deleted,

            deleted_at =
                EXCLUDED.deleted_at,

            status_priority =
                EXCLUDED.status_priority

        WHERE
            students.version <= EXCLUDED.version

            OR

            COALESCE(
                students.status_priority,
                0
            )
            <
            COALESCE(
                EXCLUDED.status_priority,
                0
            );

        """

        # ==================================================
        # EXECUTE
        # ==================================================

        print(
            "🚀 Executing student UPSERT..."
        )

        execute_batch(
            cur,
            query,
            filtered
        )

        conn.commit()

        print(
            f"✅ PostgreSQL updated successfully "
            f"({len(filtered)} students)"
        )

        # ==================================================
        # REALTIME BROADCAST
        # ==================================================

        try:

            loop = asyncio.get_running_loop()

            loop.create_task(
                broadcast_event(
                    "students"
                )
            )

        except RuntimeError:
            pass

    except Exception as e:

        conn.rollback()

        print(
            "❌ STUDENT POSTGRESQL SYNC FAILED:"
        )

        print(
            "❌ ERROR:",
            str(e)
        )

        # Print useful debugging information
        import traceback

        traceback.print_exc()

        release_db(conn)

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    release_db(conn)

    # ======================================================
    # SUCCESS RESPONSE
    # ======================================================

    return {
        "status": "success",
        "rows_processed": len(filtered)
    }
# ======================================================
# 🔥 INCREMENTAL STUDENT SYNC (CLOUD → DESKTOP SAFE)
# ======================================================

@app.get("/sync/students")
def sync_students_from_cloud(
    since: Optional[str] = Query(default=None)
):

    conn = connect_db()
    cur = conn.cursor()

    try:

        # --------------------------------------------------
        # 🔒 Validate timestamp safely
        # --------------------------------------------------
        params = ()

        base_query = """
            SELECT
                sbrn,
                name,
                semester,
                section,
                department,
                session_year,

                mobile_no,
                father_name,
                district,
                photo,

                dob,
                address,
                state,
                pincode,
                gender,
                sr_no,

                course,
                batch,
                admission_date,
                year_semester,
                academic_status,

                last_updated,
                version,
                is_deleted,
                deleted_at
            FROM students
        """

        if since:
            try:
                parsed_since = datetime.fromisoformat(since)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid 'since' timestamp format. Use ISO format."
                )

            base_query += " WHERE last_updated > %s"
            params = (parsed_since,)

        base_query += " ORDER BY last_updated ASC"

        cur.execute(base_query, params)
        rows = cur.fetchall()

    except Exception as e:
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    # --------------------------------------------------
    # 🔥 Build JSON Response Safely
    # --------------------------------------------------

    records = []
    latest_sync = None

    for r in rows:

        last_updated_val = r[21]
        deleted_at_val = r[24]

        if last_updated_val:
            latest_sync = last_updated_val.isoformat()

        records.append({
            "sbrn": r[0],
            "name": r[1],
            "semester": r[2],
            "section": r[3],
            "department": r[4],
            "session_year": r[5],

            "mobile_no": r[6],
            "father_name": r[7],
            "district": r[8],
            "photo": r[9],

            "dob": r[10],
            "address": r[11],
            "state": r[12],
            "pincode": r[13],
            "gender": r[14],
            "sr_no": r[15],

            "course": r[16],
            "batch": r[17],
            "admission_date": r[18],
            "year_semester": r[19],
            "academic_status": r[20],

            "last_updated": last_updated_val.isoformat() if last_updated_val else None,
            "version": r[22],
            "is_deleted": r[23],
            "deleted_at": deleted_at_val.isoformat() if deleted_at_val else None,
        })

    return {
        "status": "success",
        "count": len(records),
        "latest_sync": latest_sync,
        "records": records
    }

# ======================================================
# UNIVERSAL SYNC UPLOAD (AUTO PRIMARY KEY DETECTION)
# ======================================================
@app.post("/sync-generic/{table_name}")
def universal_sync_upload(table_name: str, records: list = Body(...)):

    allowed_tables = get_sync_tables()

    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail="Invalid table")

    if not records:
        return {"status": "no_data"}

    # ======================================================
    # DEBUG (VERY IMPORTANT)
    # ======================================================
    print("\n" + "=" * 80)
    print(f"🌐 SYNC REQUEST RECEIVED")
    print(f"📦 Table   : {table_name}")
    print(f"📄 Records : {len(records)}")

    if table_name == "students":
        print("\n📚 Student Payload:")
        for r in records:
            print(
                f"   SBRN={r.get('sbrn')} | "
                f"Semester={r.get('semester')} | "
                f"Version={r.get('version')} | "
                f"Sync={r.get('sync_pending')} | "
                f"Status={r.get('academic_status')}"
            )

    print("=" * 80)

    conn = connect_db()
    cur = conn.cursor()

    try:

        # ======================================================
        # STUDENT STATUS PROTECTION
        # ======================================================
        if table_name == "students":

            for row in records:

                sbrn = row.get("sbrn")

                if not sbrn:
                    continue

                cur.execute("""
                    SELECT academic_status
                    FROM students
                    WHERE sbrn=%s
                """, (sbrn,))

                existing = cur.fetchone()

                existing_status = (existing[0] if existing else "").upper()
                incoming_status = (row.get("academic_status") or "").upper()

                if existing_status == "STRUCK_OFF":

                    row["academic_status"] = (
                        "ACTIVE"
                        if incoming_status == "ACTIVE"
                        else "STRUCK_OFF"
                    )

                elif incoming_status == "STRUCK_OFF":

                    row["academic_status"] = "STRUCK_OFF"

                elif not incoming_status:

                    row["academic_status"] = (
                        existing_status if existing else "REGULAR"
                    )

                else:

                    row["academic_status"] = incoming_status

        # --------------------------------------------------
        # Detect table columns
        # --------------------------------------------------
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=%s
        """, (table_name,))

        valid_columns = {r[0] for r in cur.fetchall()}

        if not valid_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Table '{table_name}' not found."
            )

        # --------------------------------------------------
        # Detect PRIMARY KEY
        # --------------------------------------------------
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
                 ON a.attrelid=i.indrelid
                AND a.attnum=ANY(i.indkey)
            WHERE i.indrelid=%s::regclass
              AND i.indisprimary
        """, (table_name,))

        pk_columns = [r[0] for r in cur.fetchall()]

        if not pk_columns:
            raise HTTPException(
                status_code=400,
                detail=f"No primary key for {table_name}"
            )

        conflict_key = "(" + ",".join(pk_columns) + ")"

        # --------------------------------------------------
        # Collect valid columns
        # --------------------------------------------------
        all_columns = set()

        for rec in records:
            for col in rec.keys():
                if col in valid_columns:
                    all_columns.add(col)

        if table_name == "students":
            all_columns.add("academic_status")


        # ======================================================
        # 🔥 UNIVERSAL LAST_UPDATED PROTECTION
        # ======================================================

        if "last_updated" in valid_columns:

            cur.execute("""
                SELECT CURRENT_TIMESTAMP
            """)

            server_time_row = cur.fetchone()

            if server_time_row is None:
                raise RuntimeError(
                    "❌ PostgreSQL could not obtain CURRENT_TIMESTAMP"
                )

            server_now = server_time_row[0]

            for rec in records:

                if not rec.get("last_updated"):
                    rec["last_updated"] = server_now


        columns = list(all_columns)

        if not columns:
            raise HTTPException(
                status_code=400,
                detail="No valid columns supplied."
            )

        cols = ",".join(columns)

        vals = ",".join(
            [f"%({c})s" for c in columns]
        )

        update_columns = [
            c for c in columns
            if c not in pk_columns
        ]

        if update_columns:

            update_cols = ",".join(
                f"{c}=EXCLUDED.{c}"
                for c in update_columns
            )

            query = f"""
                INSERT INTO "{table_name}"
                ({cols})
                VALUES ({vals})
                ON CONFLICT {conflict_key}
                DO UPDATE SET
                    {update_cols}
            """

        else:

            # ------------------------------------------------------
            # PRIMARY-KEY-ONLY TABLE
            # Example:
            # holidays(date PRIMARY KEY)
            # ------------------------------------------------------

            query = f"""
                INSERT INTO "{table_name}"
                ({cols})
                VALUES ({vals})
                ON CONFLICT {conflict_key}
                DO NOTHING
            """

        # --------------------------------------------------
        # Ensure all keys exist
        # --------------------------------------------------
        for rec in records:

            for col in columns:

                if col not in rec:
                    rec[col] = None

        # --------------------------------------------------
        # DEBUG SQL
        # --------------------------------------------------
        print("\n🚀 Executing UPSERT...")
        print("Table :", table_name)
        print("Columns :", columns)

        execute_batch(cur, query, records)

        conn.commit()

        print(f"✅ PostgreSQL updated successfully ({len(records)} rows)")

        # --------------------------------------------------
        # Verify student update
        # --------------------------------------------------
        if table_name == "students":

            print("\n🔍 Verifying PostgreSQL Data")

            for row in records:

                sbrn = row.get("sbrn")

                cur.execute("""
                    SELECT
                        sbrn,
                        semester,
                        version,
                        academic_status
                    FROM students
                    WHERE sbrn=%s
                """, (sbrn,))

                verify = cur.fetchone()

                print("DB ROW:", verify)

        # --------------------------------------------------
        # Broadcast
        # --------------------------------------------------
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                broadcast_event(table_name)
            )
        except RuntimeError:
            pass

    except Exception as e:

        conn.rollback()

        print("\n❌ SYNC ERROR")
        print(e)

        release_db(conn)

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    release_db(conn)

    print("✅ Sync Completed\n")

    return {
        "status": "success",
        "rows": len(records)
    }


# ============================================================
# ACTIVITY ATTENDANCE — DEDICATED CLOUD SYNC
# ============================================================

@app.get("/sync/activity_attendance")
def get_activity_attendance():
    """
    Cloud → Local

    Returns all activity_attendance records from PostgreSQL.
    """

    conn = connect_db()

    try:

        cur = conn.cursor()

        cur.execute("""
            SELECT
                id,
                sbrn,
                activity_type,
                activity_name,
                date,
                weightage,
                weight_theory,
                weight_practical,
                semester,
                section,
                session_year,
                department,
                last_updated,
                version,
                sync_pending
            FROM activity_attendance
            ORDER BY
                date,
                activity_type,
                semester,
                session_year,
                sbrn
        """)

        rows = cur.fetchall()

        records = []

        for row in rows:

            records.append({
                "id": row[0],
                "sbrn": row[1],
                "activity_type": row[2],
                "activity_name": row[3],

                "date": (
                    row[4].isoformat()
                    if row[4] is not None
                    else None
                ),

                "weightage": row[5],
                "weight_theory": row[6],
                "weight_practical": row[7],

                "semester": row[8],
                "section": row[9],
                "session_year": row[10],
                "department": row[11],

                "last_updated": (
                    row[12].isoformat()
                    if row[12] is not None
                    else None
                ),

                "version": row[13],
                "sync_pending": row[14],
            })

        return {
            "status": "success",
            "table": "activity_attendance",
            "count": len(records),
            "records": records
        }

    except Exception as e:

        print(
            "❌ Activity attendance cloud GET error:",
            e
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        release_db(conn)


# ============================================================
# ACTIVITY ATTENDANCE — LOCAL → CLOUD
# ============================================================

@app.post("/sync/activity_attendance")
def sync_activity_attendance(records: list):
    """
    Local → Cloud

    Safely UPSERT activity attendance using the logical
    activity identity rather than SQLite/PostgreSQL IDs.
    """

    if not isinstance(records, list):

        raise HTTPException(
            status_code=400,
            detail="Payload must be a list."
        )

    if not records:

        return {
            "status": "success",
            "table": "activity_attendance",
            "rows_processed": 0
        }

    conn = connect_db()

    try:

        cur = conn.cursor()

        processed = 0

        for rec in records:

            if not isinstance(rec, dict):
                continue

            sbrn = str(
                rec.get("sbrn", "")
            ).strip()

            activity_type = str(
                rec.get("activity_type", "")
            ).strip()

            semester = str(
                rec.get("semester", "")
            ).strip()

            session_year = str(
                rec.get("session_year", "")
            ).strip()

            activity_date = rec.get("date")

            if not (
                sbrn
                and activity_type
                and semester
                and session_year
                and activity_date
            ):
                print(
                    "⚠ Skipping incomplete activity record:",
                    rec
                )
                continue

            cur.execute("""
                INSERT INTO activity_attendance (
                    sbrn,
                    activity_type,
                    activity_name,
                    date,
                    weightage,
                    weight_theory,
                    weight_practical,
                    semester,
                    section,
                    session_year,
                    department,
                    last_updated,
                    version,
                    sync_pending
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, 0
                )

                ON CONFLICT (
                    sbrn,
                    activity_type,
                    semester,
                    session_year,
                    date
                )

                DO UPDATE SET

                    activity_name =
                        EXCLUDED.activity_name,

                    weightage =
                        EXCLUDED.weightage,

                    weight_theory =
                        EXCLUDED.weight_theory,

                    weight_practical =
                        EXCLUDED.weight_practical,

                    section =
                        EXCLUDED.section,

                    department =
                        EXCLUDED.department,

                    last_updated =
                        EXCLUDED.last_updated,

                    version =
                        GREATEST(
                            activity_attendance.version,
                            EXCLUDED.version
                        ),

                    sync_pending = 0
            """, (
                sbrn,

                activity_type,

                rec.get("activity_name"),

                activity_date,

                rec.get("weightage", 1),

                rec.get("weight_theory", 0),

                rec.get("weight_practical", 0),

                semester,

                rec.get("section"),

                session_year,

                rec.get("department"),

                rec.get("last_updated"),

                rec.get("version", 1),
            ))

            processed += 1

        conn.commit()

        print(
            f"☁ Activity attendance accepted → "
            f"{processed} rows"
        )

        return {
            "status": "success",
            "table": "activity_attendance",
            "rows_processed": processed
        }

    except Exception as e:

        conn.rollback()

        print(
            "❌ Activity attendance cloud POST error:",
            e
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        release_db(conn)



# ============================================================
# ACTIVITY ATTENDANCE — CLOUD DELETE
# ============================================================

@app.delete("/sync/activity_attendance")
def delete_activity_attendance(records: list = Body(...)):
    """
    Local → Cloud DELETE

    Deletes specific activity attendance records using the
    logical identity rather than database IDs.
    """

    if not isinstance(records, list):

        raise HTTPException(
            status_code=400,
            detail="Payload must be a list."
        )

    if not records:

        return {
            "status": "success",
            "table": "activity_attendance",
            "rows_deleted": 0
        }

    conn = connect_db()

    try:

        cur = conn.cursor()

        deleted = 0

        for rec in records:

            if not isinstance(rec, dict):
                continue

            sbrn = str(
                rec.get("sbrn", "")
            ).strip()

            activity_type = str(
                rec.get("activity_type", "")
            ).strip()

            semester = str(
                rec.get("semester", "")
            ).strip()

            session_year = str(
                rec.get("session_year", "")
            ).strip()

            activity_date = rec.get("date")

            if not (
                sbrn
                and activity_type
                and semester
                and session_year
                and activity_date
            ):
                continue

            cur.execute("""
                DELETE FROM activity_attendance
                WHERE
                    sbrn = %s
                    AND activity_type = %s
                    AND semester = %s
                    AND session_year = %s
                    AND date = %s
            """, (
                sbrn,
                activity_type,
                semester,
                session_year,
                activity_date
            ))

            deleted += cur.rowcount

        conn.commit()

        print(
            f"☁ Activity attendance DELETE accepted → "
            f"{deleted} rows"
        )

        return {
            "status": "success",
            "table": "activity_attendance",
            "rows_deleted": deleted
        }

    except Exception as e:

        conn.rollback()

        print(
            "❌ Activity attendance cloud DELETE error:",
            e
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        release_db(conn)

# ======================================================
# UNIVERSAL SYNC DOWNLOAD (SAFE + FAST)
# ======================================================

@app.get("/sync-generic/{table_name}")
def universal_sync_download(table_name: str, since: Optional[str] = None):

    allowed_tables = get_sync_tables()

    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail="Invalid table")

    conn = connect_db()
    cur = conn.cursor()

    try:

        # --------------------------------------------------
        # Detect if table has last_updated column
        # --------------------------------------------------
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=%s
        """, (table_name,))

        columns_in_table = [r[0] for r in cur.fetchall()]
        has_last_updated = "last_updated" in columns_in_table

        params = ()

        # --------------------------------------------------
        # Build safe query
        # --------------------------------------------------
        base_query = f'SELECT * FROM "{table_name}"'

        if since and has_last_updated:

            try:
                parsed = datetime.fromisoformat(since)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid 'since' timestamp format"
                )

            base_query += " WHERE last_updated > %s"
            params = (parsed,)

        if has_last_updated:
            base_query += " ORDER BY last_updated ASC"
        else:
            base_query += " ORDER BY 1"

        cur.execute(base_query, params)

        rows = cur.fetchall() or []
        columns = [d[0] for d in cur.description] if cur.description else []

    except Exception as e:
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    records = []
    latest_sync = None

    for row in rows:

        record = dict(zip(columns, row))

        # --------------------------------------------------
        # Convert datetime → ISO format
        # --------------------------------------------------
        for k, v in record.items():
            if hasattr(v, "isoformat"):
                record[k] = v.isoformat()

        if "last_updated" in record:
            latest_sync = record["last_updated"]

        records.append(record)

    return {
        "status": "success",
        "table": table_name,
        "count": len(records),
        "latest_sync": latest_sync,
        "records": records
    }


# ======================================================
# 🔥 RESULT SUBJECTS SYNC
# LOCAL SQLITE → CLOUD POSTGRESQL
#
# IMPORTANT:
# ❌ NEVER SEND LOCAL SQLITE "id"
# ✅ CLOUD GENERATES ITS OWN SERIAL ID
# ✅ UPSERT USING:
#    (sbrn, semester, subject_id, attempt)
# ======================================================

@app.post("/sync/result_subjects")
def sync_result_subjects(records: list = Body(...)):

    if not records:
        return {
            "status": "no_data",
            "rows": 0
        }

    def safe_float(value):
        """
        Convert marks/max_marks safely.

        Grades such as A/B/S are not converted here because
        grade is stored separately.
        """
        if value is None:
            return None

        try:
            text = str(value).strip()

            if text == "":
                return None

            return float(text)

        except (ValueError, TypeError):
            return None

    conn = None
    cur = None

    clean_records = []

    try:

        conn = connect_db()
        cur = conn.cursor()

        print("\n" + "=" * 80)
        print("☁ RESULT SUBJECTS SYNC STARTED")
        print("📦 Incoming rows:", len(records))
        print("=" * 80)

        # ==================================================
        # CLEAN / VALIDATE RECORDS
        # ==================================================

        for index, r in enumerate(records, start=1):

            try:

                if not isinstance(r, dict):
                    print(
                        f"⚠ Skipping result_subjects row "
                        f"{index}: not a dictionary"
                    )
                    continue

                sbrn = r.get("sbrn")
                semester = r.get("semester")
                subject_id = r.get("subject_id")
                attempt = r.get("attempt")

                # ------------------------------------------
                # REQUIRED UNIQUE KEY
                # ------------------------------------------

                if not sbrn:
                    print(f"⚠ Skipping row {index}: missing sbrn")
                    continue

                if semester is None or str(semester).strip() == "":
                    print(f"⚠ Skipping row {index}: missing semester")
                    continue

                if not subject_id:
                    print(
                        f"⚠ Skipping row {index}: "
                        f"missing subject_id"
                    )
                    continue

                if attempt is None:
                    print(f"⚠ Skipping row {index}: missing attempt")
                    continue

                try:
                    attempt_value = int(attempt)
                except (ValueError, TypeError):
                    print(
                        f"⚠ Skipping row {index}: "
                        f"invalid attempt={attempt}"
                    )
                    continue

                # ------------------------------------------
                # NORMALIZED RECORD
                #
                # 🚨 NOTICE:
                # THERE IS NO "id" HERE.
                # ------------------------------------------

                clean = {
                    "sbrn": str(sbrn).strip(),
                    "semester": str(semester).strip(),
                    "subject_id": str(subject_id).strip(),
                    "attempt": attempt_value,

                    "marks_obtained": safe_float(
                        r.get("marks_obtained")
                    ),

                    "max_marks": safe_float(
                        r.get("max_marks")
                    ),

                    "grade": (
                        str(r.get("grade")).strip()
                        if r.get("grade") is not None
                        else None
                    ),

                    "status": (
                        str(r.get("status")).strip()
                        if r.get("status") is not None
                        else None
                    ),

                    "last_updated": (
                        r.get("last_updated")
                        or datetime.utcnow()
                    ),

                    "version": int(r.get("version") or 1)
                }

                clean_records.append(clean)

                print(
                    f"✅ SUBJECT ROW {index}: "
                    f"{clean['sbrn']} | "
                    f"Sem {clean['semester']} | "
                    f"{clean['subject_id']} | "
                    f"Attempt {clean['attempt']} | "
                    f"Grade {clean['grade']} | "
                    f"Version {clean['version']}"
                )

            except Exception as e:

                print(
                    f"❌ Failed to prepare "
                    f"result_subjects row {index}: {e}"
                )

        # ==================================================
        # NOTHING VALID
        # ==================================================

        if not clean_records:

            print("⚠ No valid result_subjects records")

            return {
                "status": "no_valid_records",
                "rows": 0
            }

        # ==================================================
        # ENSURE REQUIRED UNIQUE CONSTRAINT EXISTS
        #
        # This is the REAL logical key of a result.
        # ==================================================

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_result_subjects_sync_key
            ON result_subjects
            (
                sbrn,
                semester,
                subject_id,
                attempt
            )
        """)

        # ==================================================
        # UPSERT
        #
        # 🚨 VERY IMPORTANT:
        #
        # DO NOT INSERT "id".
        #
        # PostgreSQL generates its own ID.
        # ==================================================

        query = """
            INSERT INTO result_subjects
            (
                sbrn,
                semester,
                subject_id,
                attempt,
                marks_obtained,
                max_marks,
                grade,
                status,
                last_updated,
                version
            )
            VALUES
            (
                %(sbrn)s,
                %(semester)s,
                %(subject_id)s,
                %(attempt)s,
                %(marks_obtained)s,
                %(max_marks)s,
                %(grade)s,
                %(status)s,
                %(last_updated)s,
                %(version)s
            )

            ON CONFLICT
            (
                sbrn,
                semester,
                subject_id,
                attempt
            )

            DO UPDATE SET

                marks_obtained = EXCLUDED.marks_obtained,

                max_marks = EXCLUDED.max_marks,

                grade = EXCLUDED.grade,

                status = EXCLUDED.status,

                last_updated = EXCLUDED.last_updated,

                version = EXCLUDED.version
        """

        # ==================================================
        # EXECUTE
        # ==================================================

        execute_batch(
            cur,
            query,
            clean_records
        )

        # ==================================================
        # COMMIT ONLY AFTER SUCCESS
        # ==================================================

        conn.commit()

        print("\n" + "=" * 80)
        print(
            f"☁ RESULT SUBJECTS SYNC SUCCESS "
            f"→ {len(clean_records)} rows"
        )
        print("=" * 80)

        # ==================================================
        # OPTIONAL VERIFICATION
        # ==================================================

        for row in clean_records:

            cur.execute("""
                SELECT
                    id,
                    sbrn,
                    semester,
                    subject_id,
                    attempt,
                    grade,
                    status,
                    version,
                    last_updated
                FROM result_subjects
                WHERE sbrn=%s
                  AND semester=%s
                  AND subject_id=%s
                  AND attempt=%s
            """, (
                row["sbrn"],
                row["semester"],
                row["subject_id"],
                row["attempt"]
            ))

            db_row = cur.fetchone()

            if db_row:
                print(
                    "✅ CLOUD RESULT SUBJECT:",
                    db_row
                )

        return {
            "status": "success",
            "rows": len(clean_records)
        }

    except Exception as e:

        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

        print("\n" + "=" * 80)
        print("❌ RESULT SUBJECTS SYNC FAILED")
        print("❌ ERROR:", str(e))
        print("=" * 80)

        # IMPORTANT:
        # Returning HTTP 500 causes the desktop sync code
        # to correctly treat this as a FAILED synchronization.
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        if conn:
            try:
                release_db(conn)
            except Exception:
                pass


# ======================================================
# 🔥 RESULTS SEMESTER SYNC
# LOCAL SQLITE → CLOUD POSTGRESQL
#
# IMPORTANT:
# ❌ NEVER SEND LOCAL SQLITE "id"
# ✅ CLOUD GENERATES ITS OWN SERIAL ID
# ✅ UPSERT USING:
#    (sbrn, semester, attempt)
# ======================================================

@app.post("/sync/results_semester")
def sync_results_semester(records: list = Body(...)):

    if not records:
        return {
            "status": "no_data",
            "rows": 0
        }

    def safe_float(value):
        """
        Convert numeric values safely.
        """

        if value is None:
            return None

        try:

            text = str(value).strip()

            if text == "":
                return None

            return float(text)

        except (ValueError, TypeError):

            return None

    conn = None
    cur = None

    clean_records = []

    try:

        conn = connect_db()
        cur = conn.cursor()

        print("\n" + "=" * 80)
        print("☁ RESULTS SEMESTER SYNC STARTED")
        print("📦 Incoming rows:", len(records))
        print("=" * 80)

        # ==================================================
        # CLEAN / VALIDATE RECORDS
        # ==================================================

        for index, r in enumerate(records, start=1):

            try:

                if not isinstance(r, dict):

                    print(
                        f"⚠ Skipping semester row "
                        f"{index}: not a dictionary"
                    )

                    continue

                sbrn = r.get("sbrn")
                semester = r.get("semester")
                attempt = r.get("attempt")

                # ------------------------------------------
                # REQUIRED UNIQUE KEY
                # ------------------------------------------

                if not sbrn:

                    print(
                        f"⚠ Skipping semester row "
                        f"{index}: missing sbrn"
                    )

                    continue

                if semester is None or str(semester).strip() == "":

                    print(
                        f"⚠ Skipping semester row "
                        f"{index}: missing semester"
                    )

                    continue

                if attempt is None:

                    print(
                        f"⚠ Skipping semester row "
                        f"{index}: missing attempt"
                    )

                    continue

                try:

                    attempt_value = int(attempt)

                except (ValueError, TypeError):

                    print(
                        f"⚠ Skipping semester row "
                        f"{index}: invalid attempt={attempt}"
                    )

                    continue

                # ------------------------------------------
                # VERSION
                # ------------------------------------------

                try:

                    version_value = int(
                        r.get("version") or 1
                    )

                except (ValueError, TypeError):

                    version_value = 1

                # ------------------------------------------
                # NORMALIZED RECORD
                #
                # 🚨 NO "id"
                # ------------------------------------------

                clean = {

                    "sbrn": str(sbrn).strip(),

                    "semester": str(
                        semester
                    ).strip(),

                    "attempt": attempt_value,

                    "total_marks": safe_float(
                        r.get("total_marks")
                    ),

                    "percentage": safe_float(
                        r.get("percentage")
                    ),

                    "result_status": (
                        str(
                            r.get("result_status")
                        ).strip()
                        if r.get("result_status") is not None
                        else None
                    ),

                    "sgpa": safe_float(
                        r.get("sgpa")
                    ),

                    "created_at": (
                        r.get("created_at")
                        or datetime.utcnow()
                    ),

                    "last_updated": (
                        r.get("last_updated")
                        or datetime.utcnow()
                    ),

                    "version": version_value
                }

                clean_records.append(clean)

                print(
                    f"✅ SEMESTER ROW {index}: "
                    f"{clean['sbrn']} | "
                    f"Sem {clean['semester']} | "
                    f"Attempt {clean['attempt']} | "
                    f"SGPA {clean['sgpa']} | "
                    f"% {clean['percentage']} | "
                    f"Status {clean['result_status']} | "
                    f"Version {clean['version']}"
                )

            except Exception as e:

                print(
                    f"❌ Failed to prepare "
                    f"results_semester row {index}: {e}"
                )

        # ==================================================
        # NOTHING VALID
        # ==================================================

        if not clean_records:

            print("⚠ No valid results_semester records")

            return {
                "status": "no_valid_records",
                "rows": 0
            }

        # ==================================================
        # ENSURE UNIQUE RESULT KEY
        # ==================================================

        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            uq_results_semester_sync_key
            ON results_semester
            (
                sbrn,
                semester,
                attempt
            )
        """)

        # ==================================================
        # UPSERT
        #
        # 🚨 NO LOCAL SQLITE ID
        # ==================================================

        query = """
            INSERT INTO results_semester
            (
                sbrn,
                semester,
                attempt,
                total_marks,
                percentage,
                result_status,
                sgpa,
                created_at,
                last_updated,
                version
            )
            VALUES
            (
                %(sbrn)s,
                %(semester)s,
                %(attempt)s,
                %(total_marks)s,
                %(percentage)s,
                %(result_status)s,
                %(sgpa)s,
                %(created_at)s,
                %(last_updated)s,
                %(version)s
            )

            ON CONFLICT
            (
                sbrn,
                semester,
                attempt
            )

            DO UPDATE SET

                total_marks =
                    EXCLUDED.total_marks,

                percentage =
                    EXCLUDED.percentage,

                result_status =
                    EXCLUDED.result_status,

                sgpa =
                    EXCLUDED.sgpa,

                last_updated =
                    EXCLUDED.last_updated,

                version =
                    EXCLUDED.version

                -- IMPORTANT:
                -- created_at is NOT changed on update.
        """

        # ==================================================
        # EXECUTE
        # ==================================================

        execute_batch(
            cur,
            query,
            clean_records
        )

        # ==================================================
        # COMMIT
        # ==================================================

        conn.commit()

        print("\n" + "=" * 80)
        print(
            f"☁ RESULTS SEMESTER SYNC SUCCESS "
            f"→ {len(clean_records)} rows"
        )
        print("=" * 80)

        # ==================================================
        # VERIFY
        # ==================================================

        for row in clean_records:

            cur.execute("""
                SELECT
                    id,
                    sbrn,
                    semester,
                    attempt,
                    total_marks,
                    percentage,
                    sgpa,
                    result_status,
                    version,
                    last_updated
                FROM results_semester
                WHERE sbrn=%s
                  AND semester=%s
                  AND attempt=%s
            """, (
                row["sbrn"],
                row["semester"],
                row["attempt"]
            ))

            db_row = cur.fetchone()

            if db_row:

                print(
                    "✅ CLOUD SEMESTER RESULT:",
                    db_row
                )

        return {
            "status": "success",
            "rows": len(clean_records)
        }

    except Exception as e:

        if conn:

            try:
                conn.rollback()
            except Exception:
                pass

        print("\n" + "=" * 80)
        print("❌ RESULTS SEMESTER SYNC FAILED")
        print("❌ ERROR:", str(e))
        print("=" * 80)

        # VERY IMPORTANT:
        # HTTP 500 must reach the desktop.
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    finally:

        if conn:

            try:
                release_db(conn)
            except Exception:
                pass
# ======================================================
# 🔥 SYNC ATTENDANCE (DESKTOP → CLOUD)
# ======================================================

@app.post("/sync/attendance")
def sync_attendance_to_cloud(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    conn = connect_db()
    cur = conn.cursor()

    try:
        execute_batch(cur, """
            INSERT INTO attendance_daily
            (sbrn, subject_id, subject, semester, section, class_date, attended, last_updated)
            VALUES (%(sbrn)s, %(subject_id)s, %(subject)s,
                    %(semester)s, %(section)s,
                    %(class_date)s, %(attended)s,
                    %(last_updated)s)
            ON CONFLICT (sbrn, subject_id, semester, section, class_date)
            DO UPDATE SET
                attended = EXCLUDED.attended,
                last_updated = EXCLUDED.last_updated
            WHERE attendance_daily.last_updated < EXCLUDED.last_updated;
        """, records)

        conn.commit()

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcast_event("attendance_daily"))
        except RuntimeError:
            pass


    except Exception as e:
        conn.rollback()
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    return {"status": "success", "rows_processed": len(records)}



# ======================================================
# 🔥 INCREMENTAL ATTENDANCE SYNC (DESKTOP-ALIGNED SAFE)
# ======================================================

@app.get("/sync/attendance")
def sync_attendance_from_cloud(
    since: Optional[str] = Query(default=None)
):

    conn = connect_db()
    cur = conn.cursor()

    try:

        # --------------------------------------------------
        # 🔒 Validate timestamp safely
        # --------------------------------------------------
        if since:
            try:
                parsed_since = datetime.fromisoformat(since)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid 'since' timestamp format. Use ISO format."
                )

            cur.execute("""
                SELECT
                    sbrn,
                    subject_id,
                    subject,
                    semester,
                    section,
                    class_date,
                    attended,
                    last_updated
                FROM attendance_daily
                WHERE last_updated > %s
                ORDER BY last_updated ASC
            """, (parsed_since,))
        else:
            # First full sync
            cur.execute("""
                SELECT
                    sbrn,
                    subject_id,
                    subject,
                    semester,
                    section,
                    class_date,
                    attended,
                    last_updated
                FROM attendance_daily
                ORDER BY last_updated ASC
            """)

        rows = cur.fetchall()

    except Exception as e:
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    # --------------------------------------------------
    # 🔥 JSON SAFE RESPONSE (DESKTOP COMPATIBLE)
    # --------------------------------------------------

    data = [
        {
            "sbrn": r[0],
            "subject_id": r[1],   # 🔥 CRITICAL (desktop key)
            "subject": r[2],      # optional (readability)
            "semester": r[3],
            "section": r[4],
            "class_date": r[5].strftime("%Y-%m-%d"),
            "attended": r[6],
            "last_updated": r[7].isoformat() if r[7] else None
        }
        for r in rows
    ]

    latest_sync = None
    if rows:
        latest_sync = rows[-1][7].isoformat()

    return {
        "status": "success",
        "count": len(data),
        "latest_sync": latest_sync,
        "records": data
    }

# ======================================================
# RESET TIMETABLE (DESKTOP → CLOUD)
# ======================================================

@app.delete("/sync/timetable")
def reset_timetable_from_desktop(department: str, semester: str):

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            DELETE FROM timetable_slots
            WHERE LOWER(department)=LOWER(%s)
              AND LOWER(semester)=LOWER(%s)
        """, (department, semester))

        conn.commit()
        release_db(conn)

        return {"status": "deleted"}

    except Exception as e:
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/sync/full-reset")
@app.post("/admin/full-reset-cloud")
def full_reset_cloud(secret: str):

    # 🔐 SECURITY CHECK
    if secret != "ADMIN_RESET_123":
        raise HTTPException(status_code=403, detail="Unauthorized")

    conn = connect_db()
    cur = conn.cursor()

    try:
        # 🔍 Get all tables
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname='public'
        """)

        tables = [row[0] for row in cur.fetchall()]

        protected = {"users"}  # protect login accounts

        for table in tables:
            if table not in protected:

                print(f"⚠️ Truncating table: {table}")

                # ✅ FIX: NO semicolon
                cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')

        conn.commit()
        print("✅ CLOUD RESET COMPLETE")

    except Exception as e:
        conn.rollback()
        print("❌ RESET ERROR:", e)
        release_db(conn)
        raise HTTPException(status_code=500, detail=str(e))

    release_db(conn)

    return {"status": "cloud_reset_complete"}


# ======================================================
# 🚀 ENTERPRISE FULL DATABASE SYNC (DELTA MODE)
# ======================================================

@app.get("/sync-all")
def sync_all_tables(since: Optional[str] = None):

    conn = connect_db()
    cur = conn.cursor()

    try:

        # ---------------------------------------------
        # Get all tables automatically
        # ---------------------------------------------
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
        """)

        tables = [r[0] for r in cur.fetchall()]

        # ---------------------------------------------
        # Tables we NEVER expose
        # ---------------------------------------------
        excluded = {
            "users",
            "pg_stat_statements"
        }

        tables = [t for t in tables if t not in excluded]

        result = {}
        latest_sync = None

        for table in tables:

            try:

                # -------------------------------------
                # Check table columns
                # -------------------------------------
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name=%s
                """, (table,))

                columns_in_table = [r[0] for r in cur.fetchall()]
                has_last_updated = "last_updated" in columns_in_table

                params = ()
                query = f'SELECT * FROM "{table}"'

                # -------------------------------------
                # Delta filtering
                # -------------------------------------
                if since and has_last_updated:

                    try:
                        parsed = datetime.fromisoformat(since)
                    except Exception:
                        raise HTTPException(
                            status_code=400,
                            detail="Invalid 'since' timestamp"
                        )

                    query += " WHERE last_updated > %s"
                    params = (parsed,)

                if has_last_updated:
                    query += " ORDER BY last_updated ASC"
                else:
                    query += " ORDER BY 1"

                cur.execute(query, params)

                rows = cur.fetchall() or []
                columns = [d[0] for d in cur.description] if cur.description else []

                records = []

                for row in rows:

                    rec = dict(zip(columns, row))

                    for k, v in rec.items():
                        if hasattr(v, "isoformat"):
                            rec[k] = v.isoformat()

                    if "last_updated" in rec:
                        latest_sync = rec["last_updated"]

                    records.append(rec)

                result[table] = records

            except Exception as table_error:

                print(f"⚠ Skip table {table}: {table_error}")
                result[table] = []

        return {
            "status": "success",
            "tables": result,
            "table_count": len(result),
            "latest_sync": latest_sync
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=f"Full sync failed: {str(e)}"
        )

    finally:

        try:
            cur.close()
        except:
            pass

        try:
            release_db(conn)
        except:
            pass