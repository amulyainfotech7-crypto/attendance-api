from pydantic import BaseModel
from typing import List


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
