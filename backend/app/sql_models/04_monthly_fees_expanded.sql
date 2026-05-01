-- 04_monthly_fees_expanded.sql
-- Branch C: DC課_月額費用 × Calendar expansion
-- Contains: WEB保守サービス (ERAWAN=3267), チャットボット (ERAWAN=4034)
-- Raw date format: 2024"年"07"月" → YYYY-MM-DD

-- Step 1: Parse Japanese date strings + map ERAWAN codes
CREATE OR REPLACE TABLE monthly_fee_parsed AS
SELECT 
    "商品名",
    "加盟店名",
    -- Map ERAWAN code to full name (as DOMO ExpressionEvaluator does)
    CASE 
        WHEN "ERAWAN" = 4034 THEN '4034／WEB広告クリエイティブ'
        WHEN "ERAWAN" = 3267 THEN '3267／加盟店用WEB保守月額'
        ELSE CAST("ERAWAN" AS VARCHAR)
    END AS "ERAWAN",
    "受注月",
    "ステータス",
    "単価（円/税抜）",
    -- Parse 開始年月: YYYYMM → YYYY-MM-01
    TRY_CAST(
        CONCAT(
            LEFT(regexp_replace(CAST("開始年月" AS VARCHAR), '[^0-9]', '', 'g'), 4),
            '-',
            RIGHT(regexp_replace(CAST("開始年月" AS VARCHAR), '[^0-9]', '', 'g'), 2),
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
FROM dc_monthly_fee
WHERE "商品名" IS NOT NULL
  AND "開始年月" IS NOT NULL
  AND regexp_replace(CAST("開始年月" AS VARCHAR), '[^0-9]', '', 'g') != '';

-- Step 2: Add constants + effective end date
CREATE OR REPLACE TABLE monthly_fee_with_constants AS
SELECT 
    *,
    1073851282 AS projectId,
    '完了' AS status_name,
    10 AS customFields7_value,
    COALESCE("終了日", DATE_TRUNC('month', (SELECT ref_date FROM pipeline_config))::DATE) AS "終了日_effective"
FROM monthly_fee_parsed;

-- Step 3: INNER JOIN with calendar (expand to monthly rows)
CREATE OR REPLACE TABLE monthly_fee_expanded AS
SELECT 
    cal.calendar_date,
    cal.calendar_month_int AS calendar_month,
    mf."商品名",
    mf."加盟店名",
    mf."ERAWAN",
    mf."単価（円/税抜）",
    mf.projectId,
    mf.status_name,
    mf.customFields7_value,
    cal.calendar_date AS startDate,
    (cal.calendar_date + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS dueDate,
    CONCAT(mf."商品名", '(', LPAD(CAST(cal.calendar_month_int AS VARCHAR), 2, '0'), '月分)') AS "商品名_monthly",
    mf."開始日",
    mf."終了日"
FROM monthly_fee_with_constants mf
INNER JOIN calendar_prepared cal
    ON cal.calendar_date >= mf."開始日"
   AND cal.calendar_date <= mf."終了日_effective";

-- Step 4: Select and rename to match Backlog schema
CREATE OR REPLACE TABLE monthly_fee_schema AS
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
FROM monthly_fee_expanded
WHERE calendar_date IS NOT NULL;

-- Step 5: Add ROW_NUMBER as id (unique per product type)
CREATE OR REPLACE TABLE monthly_fee_with_id AS
SELECT *,
    ROW_NUMBER() OVER (ORDER BY customFields5_value, summary) AS id
FROM monthly_fee_schema;

-- Step 6: Filter date >= 2025-01-01
CREATE OR REPLACE TABLE monthly_fee_final AS
SELECT * FROM monthly_fee_with_id
WHERE customFields5_value >= DATE '2025-01-01';
