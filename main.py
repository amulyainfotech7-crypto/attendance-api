from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from database import connect_db
from models import LoginModel, StudentAttendance, AttendanceRequest
import hashlib
import base64
import os
from typing import List

app = FastAPI()

# ======================================================
# HEALTH CHECK (Required for Flutter App)
# ======================================================
@app.get("/health")
def health():
    return {"status": "ok"}


# ======================================================
# CORS (Flutter + Web support)
# ======================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================================================
# PASSWORD VERIFY (PBKDF2 SHA256)
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

    cur.execute("""
        SELECT DISTINCT department
        FROM students
        ORDER BY department
    """)

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
        cur.execute("""
            SELECT 1 FROM attendance_daily
            WHERE subject_id=%s
              AND semester=%s
              AND section=%s
              AND class_date=%s
            LIMIT 1
        """, (
            data.subject,
            data.semester,
            data.section,
            data.date
        ))

        already = cur.fetchone()

        if already and not data.override:
            return {
                "status": "already_marked",
                "message": "Attendance already marked"
            }

        if already:
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
