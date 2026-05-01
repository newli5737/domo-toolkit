"""
DuckDB-based ETL Engine for Dataflow 215.
Executes SQL model files in topological order against an in-memory DuckDB instance.
"""

import os
import time
import duckdb
from pathlib import Path
from dataclasses import dataclass, field
from app.core.logger import DomoLogger

_log = DomoLogger("duckdb_engine")


@dataclass
class ModelResult:
    name: str
    duration_ms: float
    row_count: int | None = None
    error: str | None = None


@dataclass
class PipelineResult:
    status: str  # "success" | "failed"
    total_duration_ms: float = 0
    output_row_count: int = 0
    output_path: str = ""
    models: list[ModelResult] = field(default_factory=list)
    error: str | None = None


class DuckDBEngine:
    """Execute SQL models against DuckDB in-memory database."""

    SQL_MODELS_DIR = Path(__file__).parent.parent / "sql_models"

    # Topological execution order
    MODEL_ORDER = [
        "00_sources",
        "01_issues_filtered",
        "02_projects_cleaned",
        "03_calendar_prepared",
        "04_monthly_fees_expanded",
        "05_subscript_lp_expanded",
        "06_excel_filtered",
        "07_union_all_sources",
        "08_post_union_processing",
        "09_cost_cumulative",
        "10_weight_pivot",
        "11_budget_history",
        "12_final_output",
    ]

    # Mapping: table_name -> filename pattern (for matching via os.listdir)
    CSV_TABLE_PATTERNS = [
        ("backlog_issue_list",    "Backlog_issue_list"),
        ("backlog_projects_list", "Backlog_Projects_list"),
        ("dc_excel_import",       "DC"),       # matches DC課_Domo取り込み...
        ("dc_monthly_fee",        None),        # resolved dynamically
        ("dc_history",            None),        # resolved dynamically  
        ("er_calendar",           "ER_calendar"),
        ("creative_budget",       None),        # resolved dynamically
        ("sub_lp",                None),        # resolved dynamically
    ]

    @staticmethod
    def _discover_csv_files(input_dir: str) -> dict[str, str]:
        """Discover CSV files and map to table names using os.listdir.
        
        This avoids hardcoded CJK strings that can be corrupted by shell encoding.
        """
        files = sorted(os.listdir(input_dir))
        csv_files = [f for f in files if f.endswith('.csv')]
        
        mapping = {}
        for f in csv_files:
            fl = f.lower()
            if 'backlog_issue_list' in fl:
                mapping['backlog_issue_list'] = f
            elif 'backlog_projects' in fl:
                mapping['backlog_projects_list'] = f
            elif fl.startswith('dc') and 'xlsx' in fl:
                mapping['dc_excel_import'] = f
            elif fl.startswith('dc') and ('月' in f or fl.count('_') == 1):
                # DC課_月額費用.csv - has 月額 in name
                if '粗利' in f or '_column_1' in fl:
                    mapping['dc_history'] = f
                else:
                    mapping['dc_monthly_fee'] = f
            elif fl.startswith('dc'):
                mapping['dc_history'] = f
            elif fl.startswith('er_'):
                mapping['er_calendar'] = f
            elif 'lp' in fl.lower():
                mapping['sub_lp'] = f
            elif 'budget' in fl or 'category' in fl or f.startswith('\u30af'):
                mapping['creative_budget'] = f
            else:
                # Fallback: check content for identification
                # Files with Japanese names we haven't matched yet
                if 'dc_monthly_fee' not in mapping:
                    mapping['dc_monthly_fee'] = f
                elif 'dc_history' not in mapping:
                    mapping['dc_history'] = f
                elif 'creative_budget' not in mapping:
                    mapping['creative_budget'] = f
                elif 'sub_lp' not in mapping:
                    mapping['sub_lp'] = f
        
        return mapping

    def _load_csv_sources(self, con: duckdb.DuckDBPyConnection, input_dir: str) -> ModelResult:
        """Load all CSV files into DuckDB tables via temp files.
        
        DuckDB on Windows cannot read file paths containing CJK characters,
        so we copy to a temp file with an ASCII-safe name and read from there.
        """
        import tempfile
        start = time.time()

        # Discover files dynamically to handle CJK filenames properly
        file_map = self._discover_csv_files(input_dir)
        
        expected_tables = [
            "backlog_issue_list", "backlog_projects_list", "dc_excel_import",
            "dc_monthly_fee", "dc_history", "er_calendar", "creative_budget", "sub_lp",
        ]
        
        for table_name in expected_tables:
            if table_name not in file_map:
                return ModelResult(
                    name="00_sources", duration_ms=0,
                    error=f"Could not find CSV for table '{table_name}'. Found: {file_map}"
                )
            filename = file_map[table_name]
            filepath = os.path.join(input_dir, filename)
            if not os.path.exists(filepath):
                return ModelResult(
                    name="00_sources", duration_ms=0,
                    error=f"File not found: {filepath}"
                )

            # Copy CSV to temp file with ASCII-safe path
            with open(filepath, "rb") as f:
                raw_bytes = f.read()

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="wb") as tmp:
                tmp.write(raw_bytes)
                tmp_path = tmp.name

            try:
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT * FROM read_csv_auto('{tmp_path}', 
                        header=true, ignore_errors=true, sample_size=-1)
                """)
                row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                _log.info(f"  📥 {table_name}: {row_count:,} rows ← {filename}")
            finally:
                os.unlink(tmp_path)

        duration = (time.time() - start) * 1000
        return ModelResult(name="00_sources", duration_ms=duration, row_count=None)

    def run(self, input_dir: str, output_path: str, reference_date: str = None) -> PipelineResult:
        """Execute all SQL models in order, export final output to CSV.
        
        Args:
            reference_date: Optional date string (YYYY-MM-DD) to use instead of CURRENT_DATE.
                           Set this to match DOMO execution date for exact comparison.
        """
        result = PipelineResult(status="running")
        total_start = time.time()

        con = duckdb.connect()  # in-memory, fast
        
        # Set reference date for reproducible results
        # SQL models should use (SELECT ref_date FROM pipeline_config) instead of CURRENT_DATE
        ref_date = reference_date or str(time.strftime('%Y-%m-%d'))
        con.execute(f"CREATE TABLE pipeline_config AS SELECT DATE '{ref_date}' AS ref_date")
        _log.info(f"Pipeline reference_date: {ref_date}")
        
        try:
            # Step 1: Load CSVs via Python IO (handles CJK filenames)
            load_result = self._load_csv_sources(con, input_dir)
            result.models.append(load_result)
            if load_result.error:
                result.status = "failed"
                result.error = load_result.error
                _log.error(result.error)
                return result

            # Step 2: Execute SQL models (skip 00_sources, already loaded)
            for model_name in self.MODEL_ORDER:
                if model_name == "00_sources":
                    continue  # already handled above

                model_result = self._execute_model(con, model_name, input_dir)
                result.models.append(model_result)

                if model_result.error:
                    result.status = "failed"
                    result.error = f"Model '{model_name}' failed: {model_result.error}"
                    _log.error(result.error)
                    break

                rows_info = f", {model_result.row_count:,} rows" if model_result.row_count else ""
                _log.info(f"  ✓ {model_name} ({model_result.duration_ms:.0f}ms{rows_info})")

            if result.status != "failed":
                # Export 1: CSV file (via temp file for CJK path compatibility)
                os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
                
                import tempfile, shutil
                with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                    tmp_out = tmp.name
                
                con.execute(f"""
                    COPY final_output TO '{tmp_out}' 
                    (HEADER, DELIMITER ',')
                """)
                
                row_count = con.execute(
                    "SELECT COUNT(*) FROM final_output"
                ).fetchone()[0]
                
                shutil.move(tmp_out, output_path)

                # Export 2: DuckDB persistent database
                # Stores output in a .duckdb file for fast querying
                # Future: migrate to ClickHouse via duckdb → clickhouse COPY
                db_path = output_path.replace(".csv", ".duckdb")
                try:
                    if os.path.exists(db_path):
                        os.remove(db_path)
                    
                    # Use a separate connection to write persistent DB
                    persist_con = duckdb.connect(db_path)
                    
                    # Get schema from in-memory table
                    schema = con.execute(
                        "SELECT * FROM final_output LIMIT 0"
                    ).description
                    
                    # Create table in persistent DB from CSV output
                    output_normalized = output_path.replace("\\", "/")
                    persist_con.execute(f"""
                        CREATE TABLE pipeline_output AS 
                        SELECT * FROM read_csv('{output_normalized}',
                            header=true, auto_detect=true, ignore_errors=true)
                    """)
                    
                    persist_cnt = persist_con.execute(
                        "SELECT COUNT(*) FROM pipeline_output"
                    ).fetchone()[0]
                    persist_con.close()
                    _log.info(f"  💾 Saved {persist_cnt:,} rows to DuckDB: {db_path}")
                except Exception as e:
                    _log.info(f"  ⚠ DuckDB persist failed (CSV still saved): {e}")

                result.status = "success"
                result.output_row_count = row_count
                result.output_path = output_path
                _log.info(f"Pipeline complete: {row_count:,} rows → {output_path}")

        except Exception as e:
            result.status = "failed"
            result.error = str(e)
            _log.error(f"Pipeline failed: {e}")
        finally:
            con.close()
            result.total_duration_ms = (time.time() - total_start) * 1000

        return result

    def _execute_model(
        self, con: duckdb.DuckDBPyConnection, model_name: str, input_dir: str
    ) -> ModelResult:
        """Execute a single SQL model file."""
        sql_path = self.SQL_MODELS_DIR / f"{model_name}.sql"

        if not sql_path.exists():
            return ModelResult(
                name=model_name, duration_ms=0, error=f"File not found: {sql_path}"
            )

        try:
            sql = sql_path.read_text(encoding="utf-8")
            # Inject input directory path (normalize path separators for DuckDB)
            normalized_dir = input_dir.replace("\\", "/")
            sql = sql.replace("${INPUT_DIR}", normalized_dir)

            start = time.time()
            # Execute multi-statement SQL
            for statement in self._split_sql(sql):
                statement = statement.strip()
                if statement and not statement.startswith("--"):
                    con.execute(statement)
            duration = (time.time() - start) * 1000

            # Try to get row count of the last created table
            row_count = self._get_last_table_count(con, sql)

            return ModelResult(
                name=model_name, duration_ms=duration, row_count=row_count
            )
        except Exception as e:
            return ModelResult(name=model_name, duration_ms=0, error=str(e))

    def _split_sql(self, sql: str) -> list[str]:
        """Split SQL into individual statements, respecting quoted strings."""
        statements = []
        current = []
        in_single_quote = False
        in_double_quote = False
        i = 0
        
        while i < len(sql):
            char = sql[i]
            
            # Handle escape sequences
            if char == "'" and not in_double_quote:
                if i + 1 < len(sql) and sql[i + 1] == "'":
                    current.append("''")
                    i += 2
                    continue
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            elif char == ';' and not in_single_quote and not in_double_quote:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
                i += 1
                continue
            elif char == '-' and not in_single_quote and not in_double_quote:
                if i + 1 < len(sql) and sql[i + 1] == '-':
                    # Skip line comment
                    while i < len(sql) and sql[i] != '\n':
                        i += 1
                    continue
            
            current.append(char)
            i += 1
        
        # Handle last statement without semicolon
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)
        
        return statements

    def _get_last_table_count(
        self, con: duckdb.DuckDBPyConnection, sql: str
    ) -> int | None:
        """Extract the last CREATE TABLE name from SQL and count its rows."""
        import re

        # Find all CREATE OR REPLACE TABLE <name> patterns
        tables = re.findall(
            r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\S+)\s+AS", sql, re.IGNORECASE
        )
        if tables:
            last_table = tables[-1]
            try:
                count = con.execute(
                    f'SELECT COUNT(*) FROM "{last_table}"'
                ).fetchone()[0]
                return count
            except:
                pass
        return None

    def preview(self, input_dir: str, limit: int = 100) -> dict:
        """Run pipeline and return preview of output data."""
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            output_path = f.name

        result = self.run(input_dir, output_path)

        if result.status != "success":
            return {"error": result.error, "models": [m.__dict__ for m in result.models]}

        # Read output for preview
        con = duckdb.connect()
        try:
            df = con.execute(
                f"SELECT * FROM read_csv('{output_path}', header=true, auto_detect=true) LIMIT {limit}"
            ).fetchdf()
            columns = list(df.columns)
            rows = df.to_dict(orient="records")
        finally:
            con.close()
            os.unlink(output_path)

        return {
            "columns": columns,
            "rows": rows,
            "total_rows": result.output_row_count,
            "duration_ms": result.total_duration_ms,
        }
