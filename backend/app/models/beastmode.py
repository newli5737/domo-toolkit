from sqlalchemy import Column, String, Integer, BigInteger, Boolean, DateTime, func
from app.core.database import Base

class BeastMode(Base):
    __tablename__ = "beastmodes"

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    owner_id = Column(BigInteger)
    legacy_id = Column(String)
    expression = Column(String)
    column_positions = Column(String)
    datasources = Column(String)
    created_at = Column(DateTime, default=func.now())

class BMCardMap(Base):
    __tablename__ = "bm_card_map"

    bm_id = Column(BigInteger, primary_key=True)
    card_id = Column(String, primary_key=True)
    is_active = Column(Boolean, default=True)

class BMDependencyMap(Base):
    __tablename__ = "bm_dependency_map"

    bm_id = Column(BigInteger, primary_key=True)
    depends_on_bm_id = Column(BigInteger, primary_key=True)

class BMAnalysis(Base):
    __tablename__ = "bm_analysis"

    bm_id = Column(BigInteger, primary_key=True)
    group_number = Column(Integer)
    group_label = Column(String)
    active_cards_count = Column(Integer, default=0)
    total_views = Column(Integer, default=0)
    referenced_by_count = Column(Integer, default=0)
    dataset_names = Column(String)
    naming_flag = Column(String)
    complexity_score = Column(Integer, default=0)
    duplicate_hash = Column(String)
    normalized_hash = Column(String)
    structure_hash = Column(String)
    url = Column(String)
    legacy_id = Column(String)
    owner_name = Column(String)
    card_ids = Column(String)

class BMDeleteLog(Base):
    __tablename__ = "bm_delete_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bm_id = Column(BigInteger, nullable=False)
    bm_name = Column(String)
    bm_legacy_id = Column(String)
    card_id = Column(String, nullable=False)
    card_definition_json = Column(String)
    status = Column(String, default='pending')
    error_message = Column(String)
    created_at = Column(DateTime, default=func.now())
