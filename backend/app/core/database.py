from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool
from app.config import get_settings

settings = get_settings()

engine = create_engine(
    settings.db_url,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def bulk_upsert(session, model, rows: list[dict], index_elements: list[str]):
    """Thực thi UPSERT an toàn cho nhiều dialect để ngăn chặn UniqueViolation."""
    if not rows:
        return
    dialect = session.bind.dialect.name
    if dialect == 'postgresql':
        from sqlalchemy.dialects.postgresql import insert
    elif dialect == 'sqlite':
        from sqlalchemy.dialects.sqlite import insert
    else:
        for r in rows:
            session.merge(model(**r))
        return

    for r in rows:
        stmt = insert(model).values(**r)
        stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={k: v for k, v in r.items() if k not in index_elements}
        )
        session.execute(stmt)
