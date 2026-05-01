-- 05_subscript_lp_expanded.sql
-- Branch D: 月額費用_サブスクLP × Calendar expansion
-- Same date fix as Branch C

-- Step 1: Filter non-null 請求開始年月, parse dates  
CREATE OR REPLACE TABLE sub_lp_parsed AS
SELECT 
    "加盟店名",
    "ステータス",
    "単価（円/税抜）",
    -- Parse 請求開始年月: YYYYMM → YYYY-MM-01
    TRY_CAST(
        CONCAT(
            LEFT(regexp_replace(CAST("請求開始年月" AS VARCHAR), '[^0-9]', '', 'g'), 4),
            '-',
            RIGHT(regexp_replace(CAST("請求開始年月" AS VARCHAR), '[^0-9]', '', 'g'), 2),
            '-01'
        )
    AS DATE) AS "開始日",
    -- Parse 終了年月
    TRY_CAST(
        CONCAT(
            LEFT(regexp_replace(CAST("終了年月" AS VARCHAR), '[^0-9]', '', 'g'), 4),
            '-',
            RIGHT(regexp_replace(CAST("終了年月" AS VARCHAR), '[^0-9]', '', 'g'), 2),
            '-01'
        )
    AS DATE) AS "終了日"
FROM sub_lp
WHERE "請求開始年月" IS NOT NULL
  AND regexp_replace(CAST("請求開始年月" AS VARCHAR), '[^0-9]', '', 'g') != '';

-- Step 2: Add constants
CREATE OR REPLACE TABLE sub_lp_with_constants AS
SELECT 
    *,
    1073851282 AS projectId,
    '完了' AS status_name,
    10 AS customFields7_value,
    'サブスクLP_月額費用' AS "商品名",
    '5917／サブクスLP' AS "ERAWAN",
    COALESCE("終了日", DATE_TRUNC('month', (SELECT ref_date FROM pipeline_config))::DATE) AS "終了日_effective"
FROM sub_lp_parsed;

-- Step 3: INNER JOIN with calendar (expand to monthly rows)
CREATE OR REPLACE TABLE sub_lp_expanded AS
SELECT 
    cal.calendar_date,
    cal.calendar_month_int AS calendar_month,
    slp."商品名",
    slp."加盟店名",
    slp."ERAWAN",
    slp."単価（円/税抜）",
    slp.projectId,
    slp.status_name,
    slp.customFields7_value,
    cal.calendar_date AS startDate,
    (cal.calendar_date + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS dueDate,
    CONCAT(slp."商品名", '(', LPAD(CAST(cal.calendar_month_int AS VARCHAR), 2, '0'), '月分)') AS "商品名_monthly"
FROM sub_lp_with_constants slp
INNER JOIN calendar_prepared cal
    ON cal.calendar_date >= slp."開始日"
   AND cal.calendar_date <= slp."終了日_effective";

-- Step 4: Select and rename to match Backlog schema
CREATE OR REPLACE TABLE sub_lp_schema AS
SELECT 
    calendar_date AS customFields5_value,
    "商品名_monthly" AS summary,
    "加盟店名" AS customFields0_value,
    CAST("ERAWAN" AS VARCHAR) AS issueType_name,
    CAST("単価（円/税抜）" AS VARCHAR) AS customFields2_value,
    projectId,
    startDate,
    dueDate,
    status_name,
    customFields7_value
FROM sub_lp_expanded
WHERE calendar_date IS NOT NULL;

-- Step 5: Add ROW_NUMBER as id
CREATE OR REPLACE TABLE sub_lp_with_id AS
SELECT *,
    ROW_NUMBER() OVER (ORDER BY customFields5_value, summary) AS id
FROM sub_lp_schema;

-- Step 6: Filter date >= 2025-01-01 AND summary NOT NULL
CREATE OR REPLACE TABLE sub_lp_final AS
SELECT * FROM sub_lp_with_id
WHERE customFields5_value >= DATE '2025-01-01'
  AND summary IS NOT NULL;
