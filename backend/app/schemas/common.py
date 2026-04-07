"""Common schemas — Generic pagination, response wrappers."""

from typing import Generic, TypeVar
from pydantic import BaseModel

T = TypeVar("T")


class PaginatedResponse(BaseModel, Generic[T]):
    """Schema phân trang chuẩn, dùng cho mọi API có list + pagination."""
    data: list[T]
    total: int
    page: int
    page_size: int
    total_pages: int
