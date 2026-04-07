from sqlalchemy import Column, String, Integer, DateTime, func
from app.core.database import Base

class MonitorCheck(Base):
    __tablename__ = "monitor_checks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    check_type = Column(String, nullable=False)
    total_checked = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    stale_count = Column(Integer, default=0)
    ok_count = Column(Integer, default=0)
    filters_json = Column(String)
    details_json = Column(String)
    checked_at = Column(DateTime, default=func.now())

class CrawlJob(Base):
    __tablename__ = "crawl_jobs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String)
    status = Column(String, default='pending')
    total = Column(Integer, default=0)
    processed = Column(Integer, default=0)
    found = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    message = Column(String)
    current_step = Column(Integer, default=0)
    total_steps = Column(Integer, default=5)
    step_name = Column(String, default='')
    step_processed = Column(Integer, default=0)
    step_total = Column(Integer, default=0)
