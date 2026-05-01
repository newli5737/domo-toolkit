"""Pipeline API Router - Trigger and manage dataflow pipeline runs."""
from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel
from typing import Optional
import os
import time
import threading

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

# ── In-memory state ──────────────────────────────────
_current_run: dict = {}
_run_lock = threading.Lock()

DATAFLOW_BASE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dataFlow")


class PipelineRunRequest(BaseModel):
    dataflow_id: str = "215"
    reference_date: Optional[str] = None  # YYYY-MM-DD


class PipelineStatusResponse(BaseModel):
    status: str  # idle | running | success | failed
    dataflow_id: Optional[str] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    duration_ms: Optional[float] = None
    output_row_count: Optional[int] = None
    output_path: Optional[str] = None
    reference_date: Optional[str] = None
    error: Optional[str] = None
    models: Optional[list] = None


class PipelineDataResponse(BaseModel):
    columns: list[str]
    data: list[dict]
    total_rows: int
    page: int
    page_size: int


def _run_pipeline(dataflow_id: str, reference_date: Optional[str]):
    """Background task: execute the DuckDB ETL pipeline."""
    global _current_run
    input_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "datainput"))
    output_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput"))
    output_csv = os.path.join(output_dir, "output.csv")

    with _run_lock:
        _current_run = {
            "status": "running",
            "dataflow_id": dataflow_id,
            "started_at": time.time(),
            "reference_date": reference_date,
        }

    try:
        from app.services.duckdb_engine import DuckDBEngine
        engine = DuckDBEngine()
        result = engine.run(input_dir, output_csv, reference_date=reference_date)

        with _run_lock:
            _current_run.update({
                "status": result.status,
                "finished_at": time.time(),
                "duration_ms": result.total_duration_ms,
                "output_row_count": result.output_row_count,
                "output_path": result.output_path,
                "error": result.error,
                "models": [
                    {
                        "name": m.name,
                        "duration_ms": round(m.duration_ms, 1),
                        "row_count": m.row_count,
                        "error": m.error,
                    }
                    for m in result.models
                ],
            })
    except Exception as e:
        with _run_lock:
            _current_run.update({
                "status": "failed",
                "finished_at": time.time(),
                "error": str(e),
            })


@router.post("/run", response_model=PipelineStatusResponse)
def trigger_pipeline(req: PipelineRunRequest, background_tasks: BackgroundTasks):
    """Trigger a pipeline run in the background."""
    with _run_lock:
        if _current_run.get("status") == "running":
            return PipelineStatusResponse(
                status="running",
                dataflow_id=_current_run.get("dataflow_id"),
                started_at=_current_run.get("started_at"),
                reference_date=_current_run.get("reference_date"),
                error="Pipeline is already running",
            )

    background_tasks.add_task(_run_pipeline, req.dataflow_id, req.reference_date)
    return PipelineStatusResponse(status="running", dataflow_id=req.dataflow_id, reference_date=req.reference_date)


@router.get("/status", response_model=PipelineStatusResponse)
def get_pipeline_status():
    """Get current pipeline run status."""
    with _run_lock:
        if not _current_run:
            return PipelineStatusResponse(status="idle")
        return PipelineStatusResponse(**_current_run)


@router.get("/data", response_model=PipelineDataResponse)
def get_pipeline_data(
    dataflow_id: str = Query("215"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=1000),
    category: Optional[str] = Query(None, description="Filter by BLカテゴリ"),
    search: Optional[str] = Query(None, description="Search 課題タイトル"),
):
    """Query pipeline output data with pagination."""
    import duckdb

    db_path = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput", "output.duckdb"))
    if not os.path.exists(db_path):
        return PipelineDataResponse(columns=[], data=[], total_rows=0, page=page, page_size=page_size)

    con = duckdb.connect(db_path, read_only=True)
    try:
        # Build WHERE clause
        conditions = []
        params = []
        if category:
            conditions.append('"BLカテゴリ" = $1')
            params.append(category)
        if search:
            idx = len(params) + 1
            conditions.append(f'"課題タイトル" LIKE ${idx}')
            params.append(f"%{search}%")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # Count
        count_sql = f"SELECT COUNT(*) FROM pipeline_output {where}"
        total = con.execute(count_sql, params).fetchone()[0]

        # Fetch page
        offset = (page - 1) * page_size
        data_sql = f"SELECT * FROM pipeline_output {where} LIMIT {page_size} OFFSET {offset}"
        result = con.execute(data_sql, params)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        # Convert to list of dicts (handle date/datetime serialization)
        data = []
        for row in rows:
            record = {}
            for col, val in zip(columns, row):
                if hasattr(val, 'isoformat'):
                    record[col] = val.isoformat()
                else:
                    record[col] = val
            data.append(record)

        return PipelineDataResponse(columns=columns, data=data, total_rows=total, page=page, page_size=page_size)
    finally:
        con.close()


@router.get("/summary")
def get_pipeline_summary(dataflow_id: str = Query("215")):
    """Get summary statistics from pipeline output."""
    import duckdb

    db_path = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput", "output.duckdb"))
    if not os.path.exists(db_path):
        return {"exists": False}

    con = duckdb.connect(db_path, read_only=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM pipeline_output").fetchone()[0]

        # By category
        categories = con.execute("""
            SELECT "BLカテゴリ", COUNT(*) as cnt 
            FROM pipeline_output GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()

        # By ERAWAN (top 10)
        erawan_top = con.execute("""
            SELECT "ERAWANコード", COUNT(*) as cnt 
            FROM pipeline_output 
            WHERE "BLカテゴリ" = '課題リスト'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """).fetchall()

        # By status
        statuses = con.execute("""
            SELECT "ステータス名", COUNT(*) as cnt 
            FROM pipeline_output 
            WHERE "BLカテゴリ" = '課題リスト'
            GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()

        # Monthly trend
        monthly = con.execute("""
            SELECT "請求年", "請求月", COUNT(*) as cnt, 
                   SUM(CAST("税抜費用（int）" AS DOUBLE)) as revenue
            FROM pipeline_output 
            WHERE "BLカテゴリ" = '課題リスト' 
              AND "請求年" IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1, 2
        """).fetchall()

        # Budget vs actual
        budget = con.execute("""
            SELECT "カテゴリ", "請求月", "売上予算額", "累計売上予算額"
            FROM pipeline_output
            WHERE "BLカテゴリ" = '予算'
            ORDER BY "カテゴリ", "請求月"
        """).fetchall()

        return {
            "exists": True,
            "total_rows": total,
            "categories": [{"name": c[0], "count": c[1]} for c in categories],
            "erawan_top": [{"name": e[0], "count": e[1]} for e in erawan_top],
            "statuses": [{"name": s[0], "count": s[1]} for s in statuses],
            "monthly": [
                {"year": m[0], "month": m[1], "count": m[2], "revenue": m[3]}
                for m in monthly
            ],
            "budget": [
                {"category": b[0], "month": b[1], "target": b[2], "cumulative": b[3]}
                for b in budget
            ],
        }
    finally:
        con.close()


@router.get("/datasets")
def get_datasets(dataflow_id: str = Query("215")):
    """List input and output datasets."""
    import duckdb

    input_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "datainput"))
    output_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput"))

    inputs = []
    if os.path.isdir(input_dir):
        for f in sorted(os.listdir(input_dir)):
            fp = os.path.join(input_dir, f)
            if os.path.isfile(fp):
                size = os.path.getsize(fp)
                rows = None
                if f.endswith(".csv"):
                    try:
                        con = duckdb.connect()
                        rows = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{fp.replace(chr(92), '/')}')").fetchone()[0]
                        con.close()
                    except Exception:
                        pass
                inputs.append({"name": f, "size_bytes": size, "rows": rows})

    outputs = []
    db_path = os.path.normpath(os.path.join(output_dir, "output.duckdb"))
    csv_path = os.path.normpath(os.path.join(output_dir, "output.csv"))
    if os.path.exists(db_path):
        try:
            con = duckdb.connect(db_path, read_only=True)
            rows = con.execute("SELECT COUNT(*) FROM pipeline_output").fetchone()[0]
            cols = con.execute("SELECT * FROM pipeline_output LIMIT 0").description
            con.close()
            outputs.append({
                "name": "output.duckdb",
                "size_bytes": os.path.getsize(db_path),
                "rows": rows,
                "columns": len(cols),
            })
        except Exception:
            pass
    if os.path.exists(csv_path):
        outputs.append({
            "name": "output.csv",
            "size_bytes": os.path.getsize(csv_path),
        })

    return {"inputs": inputs, "outputs": outputs}


@router.get("/card/yoy")
def get_card_yoy(dataflow_id: str = Query("215")):
    """Card 186671670: 売上昨対比 — Year-over-Year revenue pivot table.
    
    Beast modes translated from DOMO card definition:
    - ROW: 請求月. = concat(MONTH(請求日), '月')
    - 当年 = SUM(CASE WHEN YEAR(請求日) = YEAR(CURRENT_DATE) THEN 税抜費用(int))
    - 前年 = SUM(CASE WHEN YEAR(請求日) = YEAR(CURRENT_DATE) - 1 THEN 税抜費用(int))
    - 昨対比 = 当年 / 前年
    Filter: BLカテゴリ = '課題リスト', 請求月. IS NOT NULL/empty
    """
    import duckdb

    db_path = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput", "output.duckdb"))
    if not os.path.exists(db_path):
        return {"exists": False}

    con = duckdb.connect(db_path, read_only=True)
    try:
        result = con.execute("""
            WITH base AS (
                SELECT
                    CAST("請求日" AS DATE) AS billing_date,
                    CAST("税抜費用（int）" AS BIGINT) AS amount
                FROM pipeline_output
                WHERE "BLカテゴリ" = '課題リスト'
                  AND "請求日" IS NOT NULL
            ),
            current_year AS (
                SELECT YEAR(CURRENT_DATE) AS cy
            ),
            monthly AS (
                SELECT
                    MONTH(b.billing_date) AS m,
                    CONCAT(MONTH(b.billing_date), '月') AS month_label,
                    SUM(CASE WHEN YEAR(b.billing_date) = cy.cy THEN b.amount END) AS current_year,
                    SUM(CASE WHEN YEAR(b.billing_date) = cy.cy - 1 THEN b.amount END) AS prev_year
                FROM base b, current_year cy
                GROUP BY 1, 2, cy.cy
                HAVING CONCAT(MONTH(b.billing_date), '月') != ''
            )
            SELECT
                month_label,
                COALESCE(current_year, 0) AS current_year,
                COALESCE(prev_year, 0) AS prev_year,
                CASE
                    WHEN prev_year IS NOT NULL AND prev_year > 0
                    THEN ROUND(CAST(current_year AS DOUBLE) / prev_year, 4)
                    ELSE NULL
                END AS yoy_ratio
            FROM monthly
            ORDER BY m
        """).fetchall()

        # Totals
        total_current = sum(r[1] for r in result)
        total_prev = sum(r[2] for r in result)
        total_yoy = round(total_current / total_prev, 4) if total_prev > 0 else None

        rows = []
        for r in result:
            rows.append({
                "month": r[0],
                "current_year": r[1],
                "prev_year": r[2],
                "yoy_ratio": r[3],
            })

        return {
            "exists": True,
            "card_id": 186671670,
            "title": "売上昨対比",
            "chart_type": "badge_pivot_table",
            "rows": rows,
            "totals": {
                "current_year": total_current,
                "prev_year": total_prev,
                "yoy_ratio": total_yoy,
            },
        }
    finally:
        con.close()
