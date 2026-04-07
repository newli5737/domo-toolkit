from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.repositories.auth_repo import get_auth
from app.models.auth import DomoSession

def require_auth(db: Session = Depends(get_db)) -> DomoSession:
    """Dependency to check if the user is authenticated with Domo."""
    auth = get_auth(db)
    if not auth.is_valid:
        raise HTTPException(status_code=401, detail="Chưa login Domo.")
    return auth

def get_current_auth(db: Session = Depends(get_db)) -> DomoSession:
    """Dependency to get auth object without throwing error if not logged in."""
    return get_auth(db)
