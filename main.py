from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from database import connect_db
from models import LoginModel, AttendanceRequest
from psycopg2.extras import execute_batch
import hashlib
import base64
from datetime import datetime

app = FastAPI()

# ======================================================
# MIDDLEWARE
# ======================================================

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
# STARTUP – CREATE TABLES
# ======================================================

@app.on_event("startup")
def startup():

    conn = connect_db()
    cur = conn.cursor()

    # USERS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )
    """)

    # STUDENTS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students(
            sbrn TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT,
            semester TEXT,
            section TEXT
        )
    """)

    # SUBJECTS
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

    # 🔥 UPDATED TIMETABLE (SYNC READY)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS timetable_slots(
            id SERIAL PRIMARY KEY,
            department TEXT,
            semester TEXT,
            section TEXT,
            day TEXT,
            period_no INTEGER,
            period_len INTEGER,
            type TEXT,
            subject_id TEXT,
            faculty_id TEXT,
            room TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            version INTEGER DEFAULT 1
        )
    """)

    # Ensure unique constraint for UPSERT
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'unique_tt_slot'
            ) THEN
                ALTER TABLE timetable_slots
                ADD CONSTRAINT unique_tt_slot
                UNIQUE (department, semester, section, day, period_no);
            END IF;
        END
        $$;
    """)

    # ATTENDANCE
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance_daily(
            sbrn TEXT,
            subject_id TEXT,
            semester TEXT,
            section TEXT,
            class_date DATE,
            attended INTEGER,
            PRIMARY KEY (sbrn, subject_id, class_date, section)
        )
    """)

    conn.commit()
    conn.close()

    print("✅ PostgreSQL Server Ready (SYNC ENABLED)")

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
    conn.close()

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
# 🔥 TIMETABLE SYNC (LOCAL → CLOUD)
# ======================================================

@app.post("/sync/timetable")
def sync_timetable(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO timetable_slots
    (department, semester, section, day, period_no,
     period_len, type, subject_id, faculty_id, room,
     last_updated, version)
    VALUES (%(department)s, %(semester)s, %(section)s,
            %(day)s, %(period_no)s,
            %(period_len)s, %(type)s,
            %(subject_id)s, %(faculty_id)s, %(room)s,
            %(last_updated)s, %(version)s)
    ON CONFLICT (department, semester, section, day, period_no)
    DO UPDATE SET
        period_len = EXCLUDED.period_len,
        type = EXCLUDED.type,
        subject_id = EXCLUDED.subject_id,
        faculty_id = EXCLUDED.faculty_id,
        room = EXCLUDED.room,
        last_updated = EXCLUDED.last_updated,
        version = EXCLUDED.version
    WHERE timetable_slots.version < EXCLUDED.version;
    """

    try:
        execute_batch(cur, query, records)
        conn.commit()
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    return {"status": "success", "rows_processed": len(records)}

# ======================================================
# 🔥 GET TIMETABLE (FOR FLUTTER)
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
    conn.close()

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
