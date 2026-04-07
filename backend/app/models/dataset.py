from sqlalchemy import Column, String, Integer, BigInteger, Boolean, DateTime, func
from app.core.database import Base

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(String, primary_key=True)
    name = Column(String)
    row_count = Column(BigInteger)
    column_count = Column(Integer, default=0)
    card_count = Column(Integer, default=0)
    data_flow_count = Column(Integer, default=0)
    provider_type = Column(String)
    stream_id = Column(String)
    schedule_state = Column(String)
    dataset_status = Column(String)
    last_execution_state = Column(String)
    last_updated = Column(DateTime)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class Dataflow(Base):
    __tablename__ = "dataflows"

    id = Column(String, primary_key=True)
    name = Column(String)
    status = Column(String)
    paused = Column(Boolean, default=False)
    database_type = Column(String)
    last_execution_time = Column(DateTime)
    last_execution_state = Column(String)
    execution_count = Column(Integer, default=0)
    owner = Column(String)
    output_dataset_count = Column(Integer, default=0)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
