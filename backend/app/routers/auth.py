"""Auth Router — Thin controller cho authentication."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.repositories.auth_repo import AuthRepository, get_auth
from app.schemas.auth import LoginRequest, LoginResponse, AuthStatusResponse, LogoutResponse

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest, db: Session = Depends(get_db)):
    """Login Domo bằng username/password."""
    return AuthRepository(db).login(req.username, req.password)


@router.post("/upload-cookies", response_model=LoginResponse)
async def upload_cookies(payload: dict, db: Session = Depends(get_db)):
    """Import cookies từ J2 Team Cookie extension."""
    return AuthRepository(db).upload_cookies(payload)


@router.get("/status", response_model=AuthStatusResponse)
async def auth_status(db: Session = Depends(get_db)):
    """Kiểm tra session hiện tại."""
    return AuthRepository(db).get_status()


@router.post("/logout", response_model=LogoutResponse)
async def logout(db: Session = Depends(get_db)):
    """Đăng xuất."""
    return AuthRepository(db).logout()
