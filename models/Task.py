from datetime import datetime

from sqlalchemy import Column, DateTime, Enum, Integer, String

from .Formats import Formats
from .Status import Status
from .Base import db


class Task(db.Model):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False, default=datetime.utcnow)
    filename = Column(String(50), nullable=False)
    input_format = Column(Enum(Formats), nullable=False)
    output_format = Column(Enum(Formats))
    processed = Column(DateTime)
    inprocess = Column(DateTime)
    status = Column(Enum(Status), nullable=False, default=Status.UPLOADED)
    user_id = Column(Integer)

