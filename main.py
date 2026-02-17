from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from database import connect_db
from models import LoginModel, AttendanceRequest
import hashlib
import base64
from datetime import datetime

app = FastAPI()

# ======================================================
# HEALTH CHECK
# ======================================================
@app.get("/health")
def health():
    return {"status": "ok"}


# ======================================================
# CORS
# ======================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            active INTEGER DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS students(
            sbrn TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT,
            semester TEXT,
            section TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS subjects(
            subject_id TEXT,
            subject_name TEXT NOT NULL,
            department TEXT,
            semester TEXT,
            type TEXT,
            PRIMARY KEY (subject_id, semester)
        )
    """)

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

    print("✅ PostgreSQL Server Ready")


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

    cur.execute("SELECT DISTINCT department FROM students ORDER BY department")
    data = [r[0] for r in cur.fetchall()]

    conn.close()
    return data


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

    data = [r[0] for r in cur.fetchall()]
    conn.close()

    return data


# ======================================================
# GET SUBJECTS
# ======================================================
@app.get("/subjects")
def get_subjects(department: str, semester: str):

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT subject_id, subject_name, type
        FROM subjects
        WHERE LOWER(department)=LOWER(%s)
          AND LOWER(semester)=LOWER(%s)
        ORDER BY subject_name
    """, (department, semester))

    subjects = [
        {
            "subject_id": r[0],
            "subject_name": r[1],
            "type": r[2]
        }
        for r in cur.fetchall()
    ]

    conn.close()
    return subjects


# ======================================================
# GET STUDENTS
# ======================================================
@app.get("/students")
def get_students(department: str, semester: str, section: str = "all"):

    conn = connect_db()
    cur = conn.cursor()

    if section.lower() == "all":
        cur.execute("""
            SELECT sbrn, name
            FROM students
            WHERE LOWER(department)=LOWER(%s)
              AND LOWER(semester)=LOWER(%s)
            ORDER BY name
        """, (department, semester))
    else:
        cur.execute("""
            SELECT sbrn, name
            FROM students
            WHERE LOWER(department)=LOWER(%s)
              AND LOWER(semester)=LOWER(%s)
              AND LOWER(section)=LOWER(%s)
            ORDER BY name
        """, (department, semester, section))

    students = [{"sbrn": r[0], "name": r[1]} for r in cur.fetchall()]
    conn.close()

    return students


# ======================================================
# MARK ATTENDANCE
# ======================================================
@app.post("/mark-attendance")
def mark_attendance(data: AttendanceRequest):

    conn = connect_db()
    cur = conn.cursor()

    try:
        # Delete existing if override
        if data.override:
            cur.execute("""
                DELETE FROM attendance_daily
                WHERE subject_id=%s
                  AND semester=%s
                  AND section=%s
                  AND class_date=%s
            """, (
                data.subject,
                data.semester,
                data.section,
                data.date
            ))

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
# GET ATTENDANCE FOR DESKTOP SYNC (RAW FORMAT)
# ======================================================
@app.get("/attendance")
def get_attendance(
    department: str,
    semester: str,
    month: int,
    year: int,
    subject: str
):
    try:
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

        result = []
        for r in rows:
            result.append({
                "sbrn": r[0],
                "subject_id": r[1],
                "semester": r[2],
                "section": r[3],
                "class_date": r[4].strftime("%Y-%m-%d"),
                "attended": r[5]
            })

        conn.close()
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

