

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.core.database import get_db
from app.repositories.backlog_repo import BacklogRepository
from app.schemas.backlog import (
    BacklogPostRequest, BacklogPostResponse,
    BacklogStatusResponse, BacklogConfigResponse,
)

router = APIRouter(prefix="/api/backlog", tags=["backlog"])


@router.post("/post-status", response_model=BacklogPostResponse)
def post_backlog_status(req: BacklogPostRequest, db: Session = Depends(get_db)):
    return BacklogRepository(db).post_status(req.comment)


@router.get("/status", response_model=BacklogStatusResponse)
def backlog_api_status(db: Session = Depends(get_db)):
    return BacklogRepository(db).get_status()


@router.get("/config", response_model=BacklogConfigResponse)
def get_backlog_config(db: Session = Depends(get_db)):
    return BacklogRepository(db).get_config()
