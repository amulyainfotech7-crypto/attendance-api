import sqlite3
import psycopg2
import os
from dotenv import load_dotenv

# ==============================
# LOAD SUPABASE DATABASE URL
# ==============================
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# ==============================
# CONNECT TO SQLITE
# ==============================
sqlite_conn = sqlite3.connect("student_database.db")
sqlite_cur = sqlite_conn.cursor()

# ==============================
# CONNECT TO SUPABASE POSTGRES
# ==============================
pg_conn = psycopg2.connect(DATABASE_URL, sslmode="require")
pg_cur = pg_conn.cursor()

print("🚀 Starting Migration...\n")

# ====================================================
# 1️⃣ MIGRATE STUDENTS
# ====================================================
print("🔹 Migrating Students...")

sqlite_cur.execute("""
    SELECT sbrn, name, department, semester, section
    FROM students
""")

students = sqlite_cur.fetchall()

for s in students:
    pg_cur.execute("""
        INSERT INTO students (sbrn, name, department, semester, section)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (sbrn) DO NOTHING
    """, s)

print(f"✅ Migrated {len(students)} students\n")


# ====================================================
# 2️⃣ MIGRATE SUBJECTS WITH SEMESTER MAP
# ====================================================
print("🔹 Migrating Subjects...")

# Get subject basic info
sqlite_cur.execute("""
    SELECT subject_id, subject_name, department, type
    FROM subjects
""")
subjects = sqlite_cur.fetchall()

# Get semester mapping
sqlite_cur.execute("""
    SELECT subject_id, semester
    FROM subject_semester_map
""")
subject_sem_map = sqlite_cur.fetchall()

# Build mapping dictionary
semester_map = {}

for subject_id, semester in subject_sem_map:
    semester_map.setdefault(subject_id, []).append(semester)

count = 0

for sub in subjects:
    subject_id, subject_name, department, type_ = sub

    semesters = semester_map.get(subject_id, [])

    for semester in semesters:
        pg_cur.execute("""
            INSERT INTO subjects
            (subject_id, subject_name, department, semester, type)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (subject_id, semester) DO NOTHING
        """, (
            subject_id,
            subject_name,
            department,
            semester,
            type_
        ))
        count += 1

print(f"✅ Migrated {count} subject records\n")


# ====================================================
# COMMIT & CLOSE
# ====================================================
pg_conn.commit()

sqlite_conn.close()
pg_conn.close()

print("🎉 Migration Completed Successfully!")
