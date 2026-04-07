"""Auth schemas — Request & Response models cho Auth endpoints."""

from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str = ""
    password: str = ""


class LoginResponse(BaseModel):
    success: bool
    message: str
    username: str = ""


class AuthStatusResponse(BaseModel):
    logged_in: bool
    username: str = ""
    domo_url: str = ""


class LogoutResponse(BaseModel):
    success: bool
    message: str
