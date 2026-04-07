from sqlalchemy import Column, String, Integer, Boolean, DateTime
from app.core.database import Base

class DomoSession(Base):
    __tablename__ = "domo_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False)
    cookies_json = Column(String)
    csrf_token = Column(String)
    logged_in_at = Column(DateTime)
    is_active = Column(Boolean, default=True)

class BacklogSession(Base):
    __tablename__ = "backlog_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cookies_json = Column(String)
    csrf_token = Column(String)
    logged_in_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
