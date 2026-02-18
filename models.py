from pydantic import BaseModel
from typing import List
from datetime import datetime

# ---------------- LOGIN ----------------
class LoginModel(BaseModel):
    username: str
    password: str


# ---------------- STUDENT ----------------
class StudentModel(BaseModel):
    sbrn: str
    name: str
    department: str
    semester: str
    section: str


# ---------------- ATTENDANCE ----------------
class StudentAttendance(BaseModel):
    sbrn: str
    present: bool


class AttendanceRequest(BaseModel):
    department: str
    semester: str
    section: str
    subject: str
    date: str
    attendance: List[StudentAttendance]
    override: bool = False


# ======================================================
# 🔥 NEW: TIMETABLE SYNC MODEL
# ======================================================

class TimetableSyncRecord(BaseModel):
    department: str
    semester: str
    section: str
    day: str
    period_no: int
    period_len: int
    type: str
    subject_id: str
    faculty_id: str
    room: str
    last_updated: datetime
    version: int


class TimetableSyncRequest(BaseModel):
    records: List[TimetableSyncRecord]
