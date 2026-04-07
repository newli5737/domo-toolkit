import pytest
from unittest.mock import patch
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Patch immediately to avoid startup event using psycopg2
import app.core.database
app.core.database.engine = engine
app.core.database.SessionLocal = TestingSessionLocal

from fastapi.testclient import TestClient
from app.main import app
from app.core.database import Base, get_db
from app.dependencies import require_auth, get_current_auth
from app.models.auth import DomoSession


@pytest.fixture(scope="session")
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def db_session(setup_db):
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()
        
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

@pytest.fixture(scope="function")
def client(db_session):
    def override_get_db():
        yield db_session

    def override_require_auth():
        return DomoSession(instance="test", sid="123", token="abc", username="Admin", is_valid=True)

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[require_auth] = override_require_auth
    app.dependency_overrides[get_current_auth] = override_require_auth

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()
