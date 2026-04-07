"""QueryBuilder — Tiện ích giúp xây dựng chuỗi SQL an toàn và tái sử dụng."""

from typing import Any, Tuple


class QueryBuilder:
    def __init__(self, table: str):
        self._table = table
        self._selects = ["*"]
        self._where_conditions = []
        self._where_params = []
        self._group_bys = []
        self._order_by = ""
        self._limit = None
        self._offset = None

    def select(self, *columns: str) -> "QueryBuilder":
        """Xác định các cột cần lấy."""
        if columns:
            self._selects = list(columns)
        return self

    def where(self, condition: str, *params: Any) -> "QueryBuilder":
        """Thêm một điều kiện filter."""
        self._where_conditions.append(f"({condition})")
        self._where_params.extend(params)
        return self

    def _auto_where(self, field: str, value: Any, exact: bool = False, lower: bool = True) -> "QueryBuilder":
        """Helper tự động thêm filter ILIKE hoặc exact match."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return self
        
        if lower and isinstance(value, str):
            field_expr = f"LOWER({field})"
            val_to_check = value.strip().lower()
        else:
            field_expr = field
            val_to_check = value

        if exact:
            self.where(f"{field_expr} = %s", val_to_check)
        else:
            self.where(f"{field_expr} LIKE %s", f"%{val_to_check}%")
            
        return self

    def filter_exact(self, field: str, value: Any, lower: bool = True) -> "QueryBuilder":
        """Lọc chính xác."""
        return self._auto_where(field, value, exact=True, lower=lower)

    def filter_like(self, field: str, value: str, lower: bool = True) -> "QueryBuilder":
        """Lọc tương đối (LIKE/ILIKE)."""
        return self._auto_where(field, value, exact=False, lower=lower)

    def group_by(self, *columns: str) -> "QueryBuilder":
        """Thêm điều kiện gom nhóm."""
        self._group_bys.extend(columns)
        return self

    def order_by(self, field: str, direction: str = "DESC", nulls_last: bool = True) -> "QueryBuilder":
        """Thêm điều kiện sắp xếp."""
        if not field:
            return self
        direction = direction.upper() if direction.upper() in ("ASC", "DESC") else "DESC"
        suffix = " NULLS LAST" if nulls_last else ""
        self._order_by = f"{field} {direction}{suffix}"
        return self

    def limit_offset(self, limit: int, offset: int = 0) -> "QueryBuilder":
        """Thiết lập phân trang."""
        self._limit = limit
        self._offset = offset
        return self

    def build(self) -> Tuple[str, tuple]:
        """Tạo chuỗi SQL và tuple parameters để truyền vào psycopg2."""
        select_clause = ", ".join(self._selects)
        sql = f"SELECT {select_clause} FROM {self._table}"

        if self._where_conditions:
            sql += " WHERE " + " AND ".join(self._where_conditions)

        if self._group_bys:
            sql += " GROUP BY " + ", ".join(self._group_bys)

        if self._order_by:
            sql += " ORDER BY " + self._order_by

        if self._limit is not None:
            sql += " LIMIT %s"
            self._where_params.append(self._limit)
            if self._offset is not None:
                sql += " OFFSET %s"
                self._where_params.append(self._offset)

        return sql, tuple(self._where_params)

    def build_count(self, distinct_column: str = None) -> Tuple[str, tuple]:
        """Tạo chuỗi SQL đếm số lượng bản ghi tương ứng."""
        count_expr = f"COUNT(DISTINCT {distinct_column})" if distinct_column else "COUNT(*)"
        sql = f"SELECT {count_expr} as cnt FROM {self._table}"

        if self._where_conditions:
            sql += " WHERE " + " AND ".join(self._where_conditions)

        return sql, tuple(self._where_params)
