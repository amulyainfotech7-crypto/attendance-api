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

    # ======================================================
    # STUDENTS (FULL SYNC READY)
    # ======================================================

    cur.execute("""
        CREATE TABLE IF NOT EXISTS students(
            sbrn TEXT PRIMARY KEY,
            name TEXT,
            department TEXT,
            semester TEXT,
            section TEXT,
            course TEXT,
            batch TEXT,
            admission_date TEXT,
            year_semester TEXT,
            academic_status TEXT DEFAULT 'REGULAR',

            -- 🔥 SYNC ENGINE
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            version INTEGER DEFAULT 1,
            sync_pending INTEGER DEFAULT 0
        )
    """)

    # 🔒 Safe column repair (for old DBs)
    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS course TEXT
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS batch TEXT
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS admission_date TEXT
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS year_semester TEXT
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS academic_status TEXT DEFAULT 'REGULAR'
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS version INTEGER DEFAULT 1
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS sync_pending INTEGER DEFAULT 0
    """)

    # --------------------------------------------------
    # 🔥 SOFT DELETE SUPPORT (ENTERPRISE SAFE)
    # --------------------------------------------------

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS is_deleted INTEGER DEFAULT 0
    """)

    cur.execute("""
        ALTER TABLE students
        ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP
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

    # 🔥 TIMETABLE (FULL SYNC READY)
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

    # 🔒 Ensure UNIQUE constraint (safe execution)
    cur.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname = 'timetable_unique_slot'
        ) THEN
            ALTER TABLE timetable_slots
            ADD CONSTRAINT timetable_unique_slot
            UNIQUE (department, semester, section, day, period_no);
        END IF;
    END$$;
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
# GET DEPARTMENTS
# ======================================================

@app.get("/departments")
def get_departments():

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT department
        FROM students
        WHERE department IS NOT NULL
        ORDER BY department
    """)

    rows = cur.fetchall()
    conn.close()

    return [r[0] for r in rows]

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
    conn.close()

    return [r[0] for r in rows]

# ======================================================
# 🔥 SYNC TIMETABLE (LOCAL → CLOUD)
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
# 🔥 CLOUD → DESKTOP TIMETABLE SYNC (INCREMENTAL SAFE)
# ======================================================

from fastapi import Query
from typing import Optional

@app.get("/sync/timetable")
def get_timetable_sync(last_sync: Optional[str] = Query(default=None)):

    conn = connect_db()
    cur = conn.cursor()

    try:

        if last_sync:
            # 🔒 Safe explicit timestamp casting
            cur.execute("""
                SELECT department, semester, section, day,
                       period_no, period_len, type,
                       subject_id, faculty_id, room,
                       last_updated, version
                FROM timetable_slots
                WHERE last_updated > %s::timestamp
                ORDER BY last_updated ASC
            """, (last_sync,))
        else:
            # 🔹 First full sync
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
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    return [
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
            # 🔥 Ensure ISO format string
            "last_updated": r[10].isoformat() if r[10] else None,
            "version": r[11]
        }
        for r in rows
    ]


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

# ======================================================
# SUBJECTS BY DATE
# ======================================================

@app.get("/subjects-by-date")
def get_subjects_by_date(department: str, semester: str, date: str):

    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d")

        # ✅ short day name (Mon, Tue...)
        weekday_short = parsed_date.strftime("%a").strip()

        print("DEBUG weekday:", weekday_short)

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            t.subject_id,
            COALESCE(s.subject_name, t.subject_id) AS subject_name,
            COALESCE(s.type, t.type) AS type,
            MIN(t.period_no) as first_period
        FROM timetable_slots t
        LEFT JOIN subjects s
          ON LOWER(TRIM(t.subject_id)) = LOWER(TRIM(s.subject_id))
         AND LOWER(TRIM(t.semester))   = LOWER(TRIM(s.semester))
         AND LOWER(TRIM(t.department)) = LOWER(TRIM(s.department))
        WHERE LOWER(TRIM(t.department)) = LOWER(TRIM(%s))
          AND LOWER(TRIM(t.semester))   = LOWER(TRIM(%s))
          AND LOWER(TRIM(t.day))        = LOWER(TRIM(%s))
        GROUP BY t.subject_id, s.subject_name, s.type, t.type
        ORDER BY first_period
    """, (department, semester, weekday_short))

    rows = cur.fetchall()
    conn.close()

    print("DEBUG subjects found:", len(rows))

    return [
        {
            "subject_id": r[0],
            "subject_name": r[1],
            "type": r[2]
        }
        for r in rows
    ]


# ======================================================
# GET STUDENTS (SYNC SAFE VERSION)
# ======================================================

@app.get("/students")
def get_students(department: str, semester: str, section: str):

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT sbrn,
                   name,
                   department,
                   semester,
                   section
            FROM students
            WHERE LOWER(COALESCE(department,'')) = LOWER(%s)
              AND LOWER(COALESCE(semester,''))   = LOWER(%s)
              AND LOWER(COALESCE(section,''))    = LOWER(%s)
            ORDER BY sbrn
        """, (department, semester, section))

        rows = cur.fetchall()

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

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

# ======================================================
# 🔥 CLOUD → DESKTOP STUDENT SYNC (INCREMENTAL SAFE)
# ======================================================

from fastapi import Query
from typing import Optional

@app.get("/sync/students")
def get_students_sync(last_sync: Optional[str] = Query(default=None)):

    conn = connect_db()
    cur = conn.cursor()

    try:

        if last_sync:
            # 🔒 Explicit timestamp casting prevents type mismatch
            cur.execute("""
                SELECT sbrn, name, semester, section, department,
                       course, batch, admission_date,
                       year_semester, academic_status,
                       last_updated, version
                FROM students
                WHERE last_updated > %s::timestamp
                ORDER BY last_updated ASC
            """, (last_sync,))
        else:
            # 🔹 First full sync
            cur.execute("""
                SELECT sbrn, name, semester, section, department,
                       course, batch, admission_date,
                       year_semester, academic_status,
                       last_updated, version
                FROM students
                ORDER BY last_updated ASC
            """)

        rows = cur.fetchall()

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    return [
        {
            "sbrn": r[0],
            "name": r[1],
            "semester": r[2],
            "section": r[3],
            "department": r[4],
            "course": r[5],
            "batch": r[6],
            "admission_date": r[7],
            "year_semester": r[8],
            "academic_status": r[9],
            # 🔥 ISO format for JSON safety
            "last_updated": r[10].isoformat() if r[10] else None,
            "version": r[11]
        }
        for r in rows
    ]


# ======================================================
# CHECK ATTENDANCE EXISTS
# ======================================================

@app.get("/attendance-exists")
def attendance_exists(semester: str, section: str, subject: str, date: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT 1 FROM attendance_daily
        WHERE LOWER(semester)=LOWER(%s)
          AND LOWER(subject_id)=LOWER(%s)
          AND class_date=%s
          AND LOWER(section)=LOWER(%s)
        LIMIT 1
    """, (semester, subject, date, section))

    exists = cur.fetchone() is not None
    conn.close()

    return {"exists": exists}

# ======================================================
# MARK ATTENDANCE
# ======================================================

@app.post("/mark-attendance")
def mark_attendance(data: AttendanceRequest):

    conn = connect_db()
    cur = conn.cursor()

    try:
        for rec in data.attendance:
            cur.execute("""
                INSERT INTO attendance_daily
                (sbrn, subject_id, semester, section, class_date, attended)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sbrn, subject_id, class_date, section)
                DO UPDATE SET attended=EXCLUDED.attended
            """, (
                rec.sbrn,
                data.subject,
                data.semester,
                data.section,
                data.date,
                1 if rec.present else 0
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

    return {"status": "saved"}

# ======================================================
# GET ATTENDANCE
# ======================================================

@app.get("/attendance")
def get_attendance(department: str, semester: str, month: int, year: int, subject: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT a.sbrn,
               a.subject_id,
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
          AND LOWER(a.subject_id)=LOWER(%s)
    """, (department, semester, month, year, subject))

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "sbrn": r[0],
            "subject_id": r[1],
            "semester": r[2],
            "section": r[3],
            "class_date": r[4].strftime("%Y-%m-%d"),
            "attended": r[5]
        }
        for r in rows
    ]

# ======================================================
# 🔥 SYNC STUDENTS (LOCAL → CLOUD) — ENTERPRISE SAFE
# ======================================================

from psycopg2.extras import execute_batch
from fastapi import Body, HTTPException

@app.post("/sync/students")
def sync_students(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    # 🔥 Normalize records (prevent KeyError)
    normalized = []

    for r in records:
        normalized.append({
            "sbrn": r.get("sbrn"),
            "name": r.get("name"),
            "semester": r.get("semester"),
            "section": r.get("section"),
            "department": r.get("department"),
            "course": r.get("course"),
            "batch": r.get("batch"),
            "admission_date": r.get("admission_date"),
            "year_semester": r.get("year_semester"),
            "academic_status": r.get("academic_status", "REGULAR"),
            "last_updated": r.get("last_updated"),
            "version": r.get("version", 1),

            # 🔥 Safe defaults
            "is_deleted": r.get("is_deleted", 0),
            "deleted_at": r.get("deleted_at"),
        })

    conn = connect_db()
    cur = conn.cursor()

    query = """
    INSERT INTO students
    (
        sbrn,
        name,
        semester,
        section,
        department,
        course,
        batch,
        admission_date,
        year_semester,
        academic_status,
        last_updated,
        version,
        is_deleted,
        deleted_at
    )
    VALUES
    (
        %(sbrn)s,
        %(name)s,
        %(semester)s,
        %(section)s,
        %(department)s,
        %(course)s,
        %(batch)s,
        %(admission_date)s,
        %(year_semester)s,
        %(academic_status)s,
        %(last_updated)s,
        %(version)s,
        %(is_deleted)s,
        %(deleted_at)s
    )
    ON CONFLICT (sbrn)
    DO UPDATE SET
        name = EXCLUDED.name,
        semester = EXCLUDED.semester,
        section = EXCLUDED.section,
        department = EXCLUDED.department,
        course = EXCLUDED.course,
        batch = EXCLUDED.batch,
        admission_date = EXCLUDED.admission_date,
        year_semester = EXCLUDED.year_semester,
        academic_status = EXCLUDED.academic_status,
        last_updated = EXCLUDED.last_updated,
        version = EXCLUDED.version,
        is_deleted = EXCLUDED.is_deleted,
        deleted_at = EXCLUDED.deleted_at
    WHERE students.version < EXCLUDED.version;
    """

    try:
        execute_batch(cur, query, normalized)
        conn.commit()

    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    return {
        "status": "success",
        "rows_processed": len(normalized)
    }


# ======================================================
# 🔥 INCREMENTAL STUDENT SYNC (CLOUD → DESKTOP SAFE)
# ======================================================

from fastapi import Query, HTTPException
from typing import Optional

@app.get("/sync/students")
def sync_students_from_cloud(
    since: Optional[str] = Query(default=None)
):

    conn = connect_db()
    cur = conn.cursor()

    try:

        if since:
            # 🔒 Explicit timestamp casting prevents type mismatch
            cur.execute("""
                SELECT
                    sbrn,
                    name,
                    semester,
                    section,
                    department,
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
                WHERE last_updated > %s::timestamp
                ORDER BY last_updated ASC
            """, (since,))
        else:
            # 🔹 First full sync
            cur.execute("""
                SELECT
                    sbrn,
                    name,
                    semester,
                    section,
                    department,
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
                ORDER BY last_updated ASC
            """)

        rows = cur.fetchall()

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    # -----------------------------------------------------
    # 🔥 JSON SAFE RESPONSE
    # -----------------------------------------------------

    return [
        {
            "sbrn": r[0],
            "name": r[1],
            "semester": r[2],
            "section": r[3],
            "department": r[4],
            "course": r[5],
            "batch": r[6],
            "admission_date": r[7],
            "year_semester": r[8],
            "academic_status": r[9],

            # 🔥 ISO conversion for datetime safety
            "last_updated": r[10].isoformat() if r[10] else None,
            "version": r[11],

            # 🔥 Soft delete support
            "is_deleted": r[12],
            "deleted_at": r[13].isoformat() if r[13] else None,
        }
        for r in rows
    ]
