from fastapi import FastAPI, HTTPException, Body, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from database import connect_db
from models import LoginModel, AttendanceRequest
from psycopg2.extras import execute_batch
from fastapi import Query
from typing import Optional
import hashlib
import base64
from datetime import datetime
import calendar
from datetime import date as dt_date

app = FastAPI()

# ======================================================
# WEBSOCKET CLIENT REGISTRY
# ======================================================

connected_clients = []

# ======================================================
# MIDDLEWARE
# ======================================================
# ======================================================
# AUTO DETECT SYNC TABLES
# ======================================================

def get_sync_tables():

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    """)

    tables = [r[0] for r in cur.fetchall()]

    conn.close()

    # tables we NEVER expose
    excluded = {
        "users",
        "pg_stat_statements"
    }

    return [t for t in tables if t not in excluded]


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
# REALTIME BROADCAST
# ======================================================

import asyncio

async def broadcast_event(table_name):

    disconnected = []

    for client in connected_clients:
        try:
            await client.send_json({"table": table_name})
        except Exception:
            disconnected.append(client)

    for d in disconnected:
        if d in connected_clients:
            connected_clients.remove(d)

# ======================================================
# UNIVERSAL SYNC TABLE LIST
# ======================================================

SYNC_TABLES = {
    "students",
    "attendance_daily",
    "timetable_slots",
    "subjects",
    "semester_dates",
    "holidays"
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

    # ❌ Semester date range check
    cur.execute("""
        SELECT start_date, end_date
        FROM semester_dates
        WHERE LOWER(department)=LOWER(%s)
          AND LOWER(semester)=LOWER(%s)
    """, (department, semester))

    row = cur.fetchone()

    if row:
        start_date = row[0]
        end_date   = row[1]

        if not (start_date <= check_date <= end_date):
            conn.close()
            return False

    # ❌ Gazetted holiday check
    cur.execute(
        "SELECT 1 FROM holidays WHERE date=%s",
        (check_date,)
    )

    if cur.fetchone():
        conn.close()
        return False

    conn.close()
    return True




# ======================================================
# STARTUP – CREATE TABLES (FINAL PRODUCTION SAFE VERSION)
# ======================================================

@app.on_event("startup")
def startup():

    conn = connect_db()
    cur = conn.cursor()

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
    # STUDENTS (FULL SYNC READY)
    # ======================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS students(
            sbrn TEXT PRIMARY KEY,
            name TEXT,
            department TEXT,
            semester TEXT,
            section TEXT,

            mobile_no TEXT,
            father_name TEXT,
            district TEXT,
            photo TEXT,

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

    # Safe column repair (backward compatibility)
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
    cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS mobile_no TEXT")
    cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS father_name TEXT")
    cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS district TEXT")
    cur.execute("ALTER TABLE students ADD COLUMN IF NOT EXISTS photo TEXT")

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
    # AUTO-HEAL SUBJECTS FROM TIMETABLE
    # ======================================================
    cur.execute("""
        INSERT INTO subjects (subject_id, subject_name, department, semester, type)
        SELECT DISTINCT
            subject_id,
            subject_id,
            department,
            semester,
            type
        FROM timetable_slots
        WHERE subject_id IS NOT NULL
        ON CONFLICT DO NOTHING;
    """)

    # ======================================================
    # ATTENDANCE TABLE (DESKTOP-ALIGNED FINAL STRUCTURE)
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
    # SAFE REBUILD FOR VERY OLD DATABASES (if legacy id column exists)
    # ======================================================
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name='attendance_daily'
    """)
    columns = [row[0] for row in cur.fetchall()]

    if "id" in columns:
        print("🔄 Rebuilding legacy attendance_daily table...")

        cur.execute("ALTER TABLE attendance_daily RENAME TO attendance_old;")

        cur.execute("""
            CREATE TABLE attendance_daily(
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

        cur.execute("""
            INSERT INTO attendance_daily
            (sbrn, subject_id, subject, semester, section, class_date, attended, last_updated)
            SELECT
                sbrn,
                subject AS subject_id,
                subject,
                semester,
                section,
                class_date,
                attended,
                last_updated
            FROM attendance_old;
        """)

        cur.execute("DROP TABLE attendance_old;")
        print("✅ attendance_daily rebuilt successfully.")

    # ======================================================
    # PERFORMANCE INDEXES (SAFE)
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
        # 🔹 Main timetable sync
        execute_batch(cur, query, records)
        conn.commit()

        # ======================================================
        # 🔥 AUTO-HEAL SUBJECTS AFTER TIMETABLE SYNC (PERMANENT)
        # ======================================================
        cur.execute("""
            INSERT INTO subjects (subject_id, subject_name, department, semester, type)
            SELECT DISTINCT
                subject_id,
                subject_id,
                department,
                semester,
                type
            FROM timetable_slots
            WHERE subject_id IS NOT NULL
            ON CONFLICT DO NOTHING;
        """)
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
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

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
        # Convert to proper date object
        parsed_date = datetime.strptime(date, "%Y-%m-%d").date()

        # 🔥 BLOCK SUNDAY / 2ND SATURDAY / HOLIDAY / OUTSIDE SEMESTER
        if not is_working_day(parsed_date, department, semester):
            print("DEBUG: Holiday or Non-working day → No subjects")
            return []

        weekday_short = parsed_date.strftime("%a").strip()

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            t.subject_id,
            COALESCE(s.subject_name, t.subject_id) AS subject_name,
            COALESCE(s.type, t.type) AS type,
            MIN(t.period_no) AS first_period
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

        # ======================================================
        # 🔥 THEORY CASE → section = all → ignore section filter
        # ======================================================
        if section.lower() == "all":

            cur.execute("""
                SELECT sbrn,
                       name,
                       department,
                       semester,
                       section
                FROM students
                WHERE LOWER(COALESCE(department,'')) = LOWER(%s)
                  AND LOWER(COALESCE(semester,''))   = LOWER(%s)
                ORDER BY sbrn
            """, (department, semester))

        # ======================================================
        # 🔥 PRACTICAL CASE → filter by section
        # ======================================================
        else:

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
    conn.close()

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
                    last_updated = CURRENT_TIMESTAMP
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
        import asyncio
        asyncio.create_task(broadcast_event("attendance_daily"))

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        conn.close()

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
    conn.close()

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
# 🔥 SYNC STUDENTS (LOCAL → CLOUD) — FINAL ENTERPRISE SAFE
# ======================================================


@app.post("/sync/students")
def sync_students(records: list = Body(...)):

    if not records:
        return {"status": "no_data"}

    normalized = []

    for r in records:

        # 🔒 Critical validation
        if not r.get("sbrn"):
            continue

        normalized.append({
            "sbrn": r.get("sbrn"),
            "name": r.get("name"),
            "semester": r.get("semester"),
            "section": r.get("section"),
            "department": r.get("department"),

            "mobile_no": r.get("mobile_no"),
            "father_name": r.get("father_name"),
            "district": r.get("district"),
            "photo": r.get("photo"),

            # 🔥 NEW PROFILE FIELDS
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
            "academic_status": r.get("academic_status", "REGULAR"),

            "last_updated": r.get("last_updated") or datetime.utcnow(),
            "version": r.get("version", 1),
            "is_deleted": r.get("is_deleted", 0),
            "deleted_at": r.get("deleted_at"),
        })

    if not normalized:
        return {"status": "no_valid_records"}

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
    )
    VALUES
    (
        %(sbrn)s,
        %(name)s,
        %(semester)s,
        %(section)s,
        %(department)s,

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
        %(deleted_at)s
    )
    ON CONFLICT (sbrn)
    DO UPDATE SET
        name = EXCLUDED.name,
        semester = EXCLUDED.semester,
        section = EXCLUDED.section,
        department = EXCLUDED.department,

        mobile_no = EXCLUDED.mobile_no,
        father_name = EXCLUDED.father_name,
        district = EXCLUDED.district,
        photo = EXCLUDED.photo,

        dob = EXCLUDED.dob,
        address = EXCLUDED.address,
        state = EXCLUDED.state,
        pincode = EXCLUDED.pincode,
        gender = EXCLUDED.gender,
        sr_no = EXCLUDED.sr_no,

        course = EXCLUDED.course,
        batch = EXCLUDED.batch,
        admission_date = EXCLUDED.admission_date,
        year_semester = EXCLUDED.year_semester,
        academic_status = EXCLUDED.academic_status,

        last_updated = EXCLUDED.last_updated,
        version = EXCLUDED.version,
        is_deleted = EXCLUDED.is_deleted,
        deleted_at = EXCLUDED.deleted_at
    WHERE students.version <= EXCLUDED.version;
    """

    try:
        execute_batch(cur, query, normalized)
        conn.commit()

        import asyncio
        asyncio.create_task(broadcast_event("students"))

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
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    # --------------------------------------------------
    # 🔥 Build JSON Response Safely
    # --------------------------------------------------

    records = []
    latest_sync = None

    for r in rows:

        last_updated_val = r[20]
        deleted_at_val = r[23]

        if last_updated_val:
            latest_sync = last_updated_val.isoformat()

        records.append({
            "sbrn": r[0],
            "name": r[1],
            "semester": r[2],
            "section": r[3],
            "department": r[4],

            "mobile_no": r[5],
            "father_name": r[6],
            "district": r[7],
            "photo": r[8],

            "dob": r[9],
            "address": r[10],
            "state": r[11],
            "pincode": r[12],
            "gender": r[13],
            "sr_no": r[14],

            "course": r[15],
            "batch": r[16],
            "admission_date": r[17],
            "year_semester": r[18],
            "academic_status": r[19],

            "last_updated": last_updated_val.isoformat() if last_updated_val else None,
            "version": r[21],
            "is_deleted": r[22],
            "deleted_at": deleted_at_val.isoformat() if deleted_at_val else None,
        })

    return {
        "status": "success",
        "count": len(records),
        "latest_sync": latest_sync,
        "records": records
    }


# ======================================================
# UNIVERSAL SYNC UPLOAD
# ======================================================

@app.post("/sync-generic/{table_name}")
def universal_sync_upload(table_name: str, records: list = Body(...)):

    allowed_tables = get_sync_tables()

    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail="Invalid table")

    if not records:
        return {"status": "no_data"}

    conn = connect_db()
    cur = conn.cursor()

    try:

        columns = records[0].keys()

        cols = ",".join(columns)
        vals = ",".join([f"%({c})s" for c in columns])

        update_cols = ",".join(
            [f"{c}=EXCLUDED.{c}" for c in columns if c != "id"]
        )

        query = f"""
        INSERT INTO {table_name} ({cols})
        VALUES ({vals})
        ON CONFLICT (sbrn)
        DO UPDATE SET
        {update_cols};
        """

        execute_batch(cur, query, records)

        conn.commit()

        import asyncio
        try:
            asyncio.create_task(broadcast_event(table_name))
        except RuntimeError:
            pass

    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    return {"status": "success", "rows": len(records)}


# ======================================================
# UNIVERSAL SYNC DOWNLOAD
# ======================================================

@app.get("/sync-generic/{table_name}")
def universal_sync_download(table_name: str, since: Optional[str] = None):

    allowed_tables = get_sync_tables()

    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail="Invalid table")

    conn = connect_db()
    cur = conn.cursor()

    try:

        if since:
            parsed = datetime.fromisoformat(since)

            cur.execute(f"""
                SELECT *
                FROM {table_name}
                WHERE last_updated > %s
                ORDER BY last_updated ASC
            """, (parsed,))
        else:

            cur.execute(f"""
                SELECT *
                FROM {table_name}
                ORDER BY last_updated ASC
            """)

        rows = cur.fetchall() or []
        columns = [desc[0] for desc in cur.description] if cur.description else []

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    records = []
    latest_sync = None

    for row in rows:

        record = dict(zip(columns, row))

        if record.get("last_updated"):
            latest_sync = record["last_updated"].isoformat()
            record["last_updated"] = latest_sync

        records.append(record)

    return {
        "status": "success",
        "count": len(records),
        "latest_sync": latest_sync,
        "records": records
    }


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
        import asyncio
        asyncio.create_task(broadcast_event("attendance_daily"))


    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

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
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

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
        conn.close()

        return {"status": "deleted"}

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/sync/full-reset")
def full_reset_cloud(secret: str):

    # 🔐 SECURITY CHECK
    if secret != "ADMIN_RESET_123":
        raise HTTPException(status_code=403, detail="Unauthorized")

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname='public';
        """)

        tables = [row[0] for row in cur.fetchall()]

        protected = {"users"}  # protect login accounts

        for table in tables:
            if table not in protected:
                cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;')

        conn.commit()

    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

    conn.close()

    return {"status": "cloud_reset_complete"}
