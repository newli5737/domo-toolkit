from sqlalchemy import Column, String, Integer, BigInteger, DateTime
from app.core.database import Base

class Card(Base):
    __tablename__ = "cards"

    id = Column(String, primary_key=True, index=True)
    title = Column(String, index=True)
    card_type = Column(String, index=True)
    view_count = Column(Integer, default=0)
    last_viewed_at = Column(DateTime)
    owner_name = Column(String, index=True)
    page_id = Column(BigInteger, index=True)
    page_title = Column(String, index=True)
    datasource_id = Column(String, index=True)
    datasource_name = Column(String, index=True)
