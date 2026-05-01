"""Pipeline API Router - Trigger and manage dataflow pipeline runs."""
from fastapi import APIRouter, BackgroundTasks, Query
from pydantic import BaseModel
from typing import Optional
import os, time, threading, json

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

_current_run: dict = {}
_run_lock = threading.Lock()
DATAFLOW_BASE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "dataFlow"))


def _load_config(dataflow_id: str) -> dict:
    cfg_path = os.path.join(DATAFLOW_BASE, dataflow_id, "config.json")
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"name": f"Dataflow {dataflow_id}", "output_display_name": "output", "cards": []}


def _save_config(dataflow_id: str, cfg: dict):
    cfg_path = os.path.join(DATAFLOW_BASE, dataflow_id, "config.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _db_path(dataflow_id: str) -> str:
    return os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput", "output.duckdb"))


# ── Models ──────────────────────────────────────────────

class PipelineRunRequest(BaseModel):
    dataflow_id: str = "215"
    reference_date: Optional[str] = None

class PipelineStatusResponse(BaseModel):
    status: str
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


# ── Pipeline execution ──────────────────────────────────

def _run_pipeline(dataflow_id: str, reference_date: Optional[str]):
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
                    {"name": m.name, "duration_ms": round(m.duration_ms, 1), "row_count": m.row_count, "error": m.error}
                    for m in result.models
                ],
            })
    except Exception as e:
        with _run_lock:
            _current_run.update({"status": "failed", "finished_at": time.time(), "error": str(e)})


@router.post("/run", response_model=PipelineStatusResponse)
def trigger_pipeline(req: PipelineRunRequest, background_tasks: BackgroundTasks):
    with _run_lock:
        if _current_run.get("status") == "running":
            return PipelineStatusResponse(
                status="running", dataflow_id=_current_run.get("dataflow_id"),
                started_at=_current_run.get("started_at"), reference_date=_current_run.get("reference_date"),
                error="Pipeline is already running",
            )
    background_tasks.add_task(_run_pipeline, req.dataflow_id, req.reference_date)
    return PipelineStatusResponse(status="running", dataflow_id=req.dataflow_id, reference_date=req.reference_date)


@router.get("/status", response_model=PipelineStatusResponse)
def get_pipeline_status():
    with _run_lock:
        if not _current_run:
            return PipelineStatusResponse(status="idle")
        return PipelineStatusResponse(**_current_run)


# ── List dataflows ──────────────────────────────────────

@router.get("/list")
def list_dataflows():
    """List all available dataflow pipelines."""
    result = []
    if os.path.isdir(DATAFLOW_BASE):
        for d in sorted(os.listdir(DATAFLOW_BASE)):
            dp = os.path.join(DATAFLOW_BASE, d)
            if os.path.isdir(dp) and os.path.isdir(os.path.join(dp, "datainput")):
                cfg = _load_config(d)
                has_output = os.path.exists(os.path.join(dp, "dataoutput", "output.duckdb"))
                result.append({
                    "id": d,
                    "name": cfg.get("name", f"Dataflow {d}"),
                    "output_display_name": cfg.get("output_display_name", "output"),
                    "has_output": has_output,
                    "card_count": len(cfg.get("cards", [])),
                })
    return result


# ── Sync from DOMO ──────────────────────────────────────

_sync_status: dict = {}

def _do_sync(dataflow_id: str):
    """Download all input datasets from DOMO using the export API."""
    global _sync_status
    import requests as req_lib
    from app.repositories.auth_repo import get_auth

    cfg = _load_config(dataflow_id)
    inputs = cfg.get("inputs", [])
    if not inputs:
        _sync_status = {"status": "failed", "error": "No inputs configured in config.json"}
        return

    auth = get_auth()
    if not auth.is_valid:
        _sync_status = {"status": "failed", "error": "DOMO session expired. Please login first."}
        return

    base_url = f"https://{auth.instance}"
    input_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "datainput"))
    os.makedirs(input_dir, exist_ok=True)

    session = req_lib.Session()
    for k, v in auth.cookies.items():
        session.cookies.set(k, v)

    results = []
    for i, inp in enumerate(inputs):
        domo_id = inp.get("domo_id", "")
        filename = inp.get("file", "")
        name = inp.get("name", filename)
        _sync_status["current"] = f"{name} ({i+1}/{len(inputs)})"

        if not domo_id or not filename:
            results.append({"name": name, "status": "skipped", "error": "Missing domo_id or file"})
            continue

        try:
            url = f"{base_url}/api/query/v1/execute/export/{domo_id}"
            params = {"accept": "text/csv", "disableFormulaInterpretation": "true", "includeHeader": "true"}
            r = session.get(url, params=params, timeout=120)
            if r.status_code == 200 and "text/csv" in r.headers.get("content-type", ""):
                filepath = os.path.join(input_dir, filename)
                with open(filepath, "wb") as f:
                    f.write(r.content)
                rows = r.content.count(b"\n") - 1
                results.append({"name": name, "file": filename, "status": "ok", "size": len(r.content), "rows": rows})
            else:
                results.append({"name": name, "status": "error", "http_status": r.status_code, "error": r.text[:200]})
        except Exception as e:
            results.append({"name": name, "status": "error", "error": str(e)})

    ok = sum(1 for r in results if r["status"] == "ok")
    _sync_status = {
        "status": "success" if ok == len(inputs) else "partial",
        "finished_at": time.time(),
        "total": len(inputs), "ok": ok,
        "results": results,
    }


@router.post("/sync")
def sync_from_domo(req: PipelineRunRequest, background_tasks: BackgroundTasks):
    """Download fresh input CSVs from DOMO."""
    global _sync_status
    if _sync_status.get("status") == "syncing":
        return {"status": "syncing", "message": "Sync already running", "current": _sync_status.get("current", "")}
    _sync_status = {"status": "syncing", "started_at": time.time()}
    background_tasks.add_task(_do_sync, req.dataflow_id)
    return {"status": "syncing", "message": "Sync started"}


@router.get("/sync/status")
def get_sync_status():
    return _sync_status or {"status": "idle"}



@router.get("/data", response_model=PipelineDataResponse)
def get_pipeline_data(
    dataflow_id: str = Query("215"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=10, le=1000),
    category: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    import duckdb
    db = _db_path(dataflow_id)
    if not os.path.exists(db):
        return PipelineDataResponse(columns=[], data=[], total_rows=0, page=page, page_size=page_size)

    con = duckdb.connect(db, read_only=True)
    try:
        conditions, params = [], []
        if category:
            conditions.append('"BLカテゴリ" = $1')
            params.append(category)
        if search:
            idx = len(params) + 1
            conditions.append(f'"課題タイトル" LIKE ${idx}')
            params.append(f"%{search}%")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        total = con.execute(f"SELECT COUNT(*) FROM pipeline_output {where}", params).fetchone()[0]
        offset = (page - 1) * page_size
        result = con.execute(f"SELECT * FROM pipeline_output {where} LIMIT {page_size} OFFSET {offset}", params)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        data = []
        for row in rows:
            record = {}
            for col, val in zip(columns, row):
                record[col] = val.isoformat() if hasattr(val, 'isoformat') else val
            data.append(record)
        return PipelineDataResponse(columns=columns, data=data, total_rows=total, page=page, page_size=page_size)
    finally:
        con.close()


# ── Summary ─────────────────────────────────────────────

@router.get("/summary")
def get_pipeline_summary(dataflow_id: str = Query("215")):
    import duckdb
    db = _db_path(dataflow_id)
    if not os.path.exists(db):
        return {"exists": False}

    con = duckdb.connect(db, read_only=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM pipeline_output").fetchone()[0]
        categories = con.execute('SELECT "BLカテゴリ", COUNT(*) FROM pipeline_output GROUP BY 1 ORDER BY 2 DESC').fetchall()
        erawan_top = con.execute("""
            SELECT "ERAWANコード", COUNT(*) FROM pipeline_output 
            WHERE "BLカテゴリ" = '課題リスト' GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """).fetchall()
        statuses = con.execute("""
            SELECT "ステータス名", COUNT(*) FROM pipeline_output 
            WHERE "BLカテゴリ" = '課題リスト' GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()
        monthly = con.execute("""
            SELECT "請求年", "請求月", COUNT(*), SUM(CAST("税抜費用（int）" AS DOUBLE))
            FROM pipeline_output WHERE "BLカテゴリ" = '課題リスト' AND "請求年" IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1, 2
        """).fetchall()
        budget = con.execute("""
            SELECT "カテゴリ", "請求月", "売上予算額", "累計売上予算額"
            FROM pipeline_output WHERE "BLカテゴリ" = '予算' ORDER BY "カテゴリ", "請求月"
        """).fetchall()

        return {
            "exists": True, "total_rows": total,
            "categories": [{"name": c[0], "count": c[1]} for c in categories],
            "erawan_top": [{"name": e[0], "count": e[1]} for e in erawan_top],
            "statuses": [{"name": s[0], "count": s[1]} for s in statuses],
            "monthly": [{"year": m[0], "month": m[1], "count": m[2], "revenue": m[3]} for m in monthly],
            "budget": [{"category": b[0], "month": b[1], "target": b[2], "cumulative": b[3]} for b in budget],
        }
    finally:
        con.close()


# ── Datasets ────────────────────────────────────────────

@router.get("/datasets")
def get_datasets(dataflow_id: str = Query("215")):
    import duckdb

    input_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "datainput"))
    output_dir = os.path.normpath(os.path.join(DATAFLOW_BASE, dataflow_id, "dataoutput"))
    cfg = _load_config(dataflow_id)

    inputs = []
    if os.path.isdir(input_dir):
        for f in sorted(os.listdir(input_dir)):
            fp = os.path.join(input_dir, f)
            if os.path.isfile(fp):
                size = os.path.getsize(fp)
                rows = None
                if f.endswith(".csv"):
                    try:
                        c = duckdb.connect()
                        rows = c.execute(f"SELECT COUNT(*) FROM read_csv_auto('{fp.replace(chr(92), '/')}')").fetchone()[0]
                        c.close()
                    except Exception:
                        pass
                inputs.append({"name": f, "size_bytes": size, "rows": rows})

    outputs = []
    db = _db_path(dataflow_id)
    csv_path = os.path.normpath(os.path.join(output_dir, "output.csv"))
    if os.path.exists(db):
        try:
            c = duckdb.connect(db, read_only=True)
            rows = c.execute("SELECT COUNT(*) FROM pipeline_output").fetchone()[0]
            cols = c.execute("SELECT * FROM pipeline_output LIMIT 0").description
            c.close()
            outputs.append({
                "name": "output.duckdb",
                "display_name": cfg.get("output_display_name", "output"),
                "size_bytes": os.path.getsize(db),
                "rows": rows,
                "columns": len(cols),
                "cards": cfg.get("cards", []),
                "last_modified": os.path.getmtime(db),
            })
        except Exception:
            pass
    if os.path.exists(csv_path):
        outputs.append({
            "name": "output.csv",
            "display_name": cfg.get("output_display_name", "output"),
            "size_bytes": os.path.getsize(csv_path),
        })

    return {"inputs": inputs, "outputs": outputs}


@router.get("/datasets/detail")
def get_dataset_detail(dataflow_id: str = Query("215")):
    """Full column info, stats, linked cards for output dataset."""
    import duckdb

    db = _db_path(dataflow_id)
    if not os.path.exists(db):
        return {"exists": False}

    cfg = _load_config(dataflow_id)
    con = duckdb.connect(db, read_only=True)
    try:
        total = con.execute("SELECT COUNT(*) FROM pipeline_output").fetchone()[0]
        desc = con.execute("SELECT * FROM pipeline_output LIMIT 0").description
        sample = con.execute("SELECT * FROM pipeline_output LIMIT 3").fetchall()

        columns = []
        for i, d in enumerate(desc):
            col_name = d[0]
            col_type = str(d[1])
            samples = [str(row[i]) if row[i] is not None else None for row in sample]
            nulls = con.execute(f'SELECT COUNT(*) FROM pipeline_output WHERE "{col_name}" IS NULL').fetchone()[0]
            distinct = con.execute(f'SELECT COUNT(DISTINCT "{col_name}") FROM pipeline_output').fetchone()[0]
            columns.append({
                "name": col_name, "type": col_type, "samples": samples,
                "null_count": nulls, "distinct_count": distinct,
            })

        return {
            "exists": True,
            "display_name": cfg.get("output_display_name", "output"),
            "total_rows": total,
            "column_count": len(desc),
            "columns": columns,
            "cards": cfg.get("cards", []),
            "last_modified": os.path.getmtime(db),
            "size_bytes": os.path.getsize(db),
        }
    finally:
        con.close()


class RenameRequest(BaseModel):
    dataflow_id: str = "215"
    display_name: str

@router.put("/datasets/rename")
def rename_dataset(req: RenameRequest):
    cfg = _load_config(req.dataflow_id)
    cfg["output_display_name"] = req.display_name
    _save_config(req.dataflow_id, cfg)
    return {"ok": True, "display_name": req.display_name}


# ── Card filter values ──────────────────────────────────

@router.get("/card/filters")
def get_card_filters(dataflow_id: str = Query("215")):
    """Get distinct filter values for key columns (smart: only columns with <200 distinct)."""
    import duckdb
    db = _db_path(dataflow_id)
    if not os.path.exists(db):
        return {"filters": []}

    filter_columns = [
        "BLカテゴリ", "ステータス名", "ERAWANコード", "プロジェクト名",
        "カテゴリ", "担当者種別", "種", "請求日（期）",
    ]

    con = duckdb.connect(db, read_only=True)
    try:
        result = []
        all_cols = [d[0] for d in con.execute("SELECT * FROM pipeline_output LIMIT 0").description]
        for col in filter_columns:
            if col not in all_cols:
                continue
            distinct = con.execute(f'SELECT COUNT(DISTINCT "{col}") FROM pipeline_output').fetchone()[0]
            if distinct > 200:
                continue
            values = con.execute(f"""
                SELECT "{col}", COUNT(*) as cnt FROM pipeline_output
                WHERE "{col}" IS NOT NULL AND "{col}" != ''
                GROUP BY 1 ORDER BY 2 DESC
            """).fetchall()
            result.append({
                "column": col,
                "values": [{"value": v[0], "count": v[1]} for v in values],
            })
        return {"filters": result}
    finally:
        con.close()


# ── Card: 売上昨対比 (186671670) ────────────────────────

@router.get("/card/yoy")
def get_card_yoy(
    dataflow_id: str = Query("215"),
    bl_category: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    erawan: Optional[str] = Query(None),
    project: Optional[str] = Query(None),
):
    """Card 186671670: YoY revenue pivot with optional filters."""
    import duckdb
    db = _db_path(dataflow_id)
    if not os.path.exists(db):
        return {"exists": False}

    con = duckdb.connect(db, read_only=True)
    try:
        filters = ['"BLカテゴリ" = \'課題リスト\'', '"請求日" IS NOT NULL']
        if bl_category:
            filters[0] = f'"BLカテゴリ" = \'{bl_category}\''
        if status_name:
            filters.append(f'"ステータス名" = \'{status_name}\'')
        if erawan:
            filters.append(f'"ERAWANコード" = \'{erawan}\'')
        if project:
            filters.append(f'"プロジェクト名" = \'{project}\'')
        where = " AND ".join(filters)

        result = con.execute(f"""
            WITH base AS (
                SELECT CAST("請求日" AS DATE) AS bd, CAST("税抜費用（int）" AS BIGINT) AS amt
                FROM pipeline_output WHERE {where}
            ),
            cy AS (SELECT YEAR(CURRENT_DATE) AS y),
            monthly AS (
                SELECT MONTH(b.bd) AS m, CONCAT(MONTH(b.bd), '月') AS ml,
                    SUM(CASE WHEN YEAR(b.bd) = cy.y THEN b.amt END) AS cur,
                    SUM(CASE WHEN YEAR(b.bd) = cy.y - 1 THEN b.amt END) AS prev
                FROM base b, cy GROUP BY 1, 2, cy.y
                HAVING ml != ''
            )
            SELECT ml, COALESCE(cur, 0), COALESCE(prev, 0),
                CASE WHEN prev > 0 THEN ROUND(CAST(cur AS DOUBLE) / prev, 4) END
            FROM monthly ORDER BY m
        """).fetchall()

        total_cur = sum(r[1] for r in result)
        total_prev = sum(r[2] for r in result)
        return {
            "exists": True, "card_id": 186671670, "title": "売上昨対比",
            "chart_type": "badge_pivot_table",
            "rows": [{"month": r[0], "current_year": r[1], "prev_year": r[2], "yoy_ratio": r[3]} for r in result],
            "totals": {
                "current_year": total_cur, "prev_year": total_prev,
                "yoy_ratio": round(total_cur / total_prev, 4) if total_prev > 0 else None,
            },
        }
    finally:
        con.close()


# ── Card: 月別・ERAWANコード別売上額 (258978026) ───────

@router.get("/card/revenue-by-year")
def get_card_revenue_by_year(
    dataflow_id: str = Query("215"),
    bl_category: Optional[str] = Query(None),
    status_name: Optional[str] = Query(None),
    erawan: Optional[str] = Query(None),
    project: Optional[str] = Query(None),
):
    """Card 258978026: Revenue by month pivoted by year.
    ROW=請求月(concat MONTH, '月'), COLUMN=Year, VALUE=SUM(税抜費用)"""
    import duckdb
    db = _db_path(dataflow_id)
    if not os.path.exists(db):
        return {"exists": False}

    con = duckdb.connect(db, read_only=True)
    try:
        filters = ['"BLカテゴリ" = \'課題リスト\'', '"ERAWANコード" IS NOT NULL', '"ERAWANコード" != \'\'', '"請求日" IS NOT NULL']
        if bl_category:
            filters[0] = f'"BLカテゴリ" = \'{bl_category}\''
        if status_name:
            filters.append(f'"ステータス名" = \'{status_name}\'')
        if erawan:
            filters.append(f'"ERAWANコード" = \'{erawan}\'')
        if project:
            filters.append(f'"プロジェクト名" = \'{project}\'')
        where = " AND ".join(filters)

        raw = con.execute(f"""
            SELECT MONTH(CAST("請求日" AS DATE)) AS m,
                   CONCAT(MONTH(CAST("請求日" AS DATE)), '月') AS month_label,
                   YEAR(CAST("請求日" AS DATE)) AS yr,
                   SUM(CAST("税抜費用（int）" AS BIGINT)) AS revenue
            FROM pipeline_output WHERE {where}
            GROUP BY 1, 2, 3 ORDER BY 1, 3
        """).fetchall()

        years = sorted(set(r[2] for r in raw))
        months = []
        seen = set()
        for r in raw:
            if r[1] not in seen:
                seen.add(r[1])
                months.append({"m": r[0], "label": r[1]})
        months.sort(key=lambda x: x["m"])

        rows = []
        year_totals = {y: 0 for y in years}
        for month in months:
            row = {"month": month["label"]}
            for y in years:
                val = next((r[3] for r in raw if r[0] == month["m"] and r[2] == y), 0)
                row[str(y)] = val or 0
                year_totals[y] += val or 0
            rows.append(row)

        return {
            "exists": True, "card_id": 258978026, "title": "月別・ERAWANコード別売上額",
            "chart_type": "badge_pivot_table",
            "years": years,
            "rows": rows,
            "totals": {str(y): year_totals[y] for y in years},
        }
    finally:
        con.close()
