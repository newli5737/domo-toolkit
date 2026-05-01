-- 11_budget_history.sql
-- Budget branch (クリエイティブ予算) + Historical data branch (DC課_粗利過去データ)

-- ============================================================
-- BUDGET BRANCH
-- ============================================================

-- Step 1: Remove batch columns, rename year/category
CREATE OR REPLACE TABLE budget_cleaned AS
SELECT 
    CAST(year AS INTEGER) AS "請求年",
    category AS "カテゴリ",
    TRY_CAST("1" AS BIGINT) AS m1,
    TRY_CAST("2" AS BIGINT) AS m2,
    TRY_CAST("3" AS BIGINT) AS m3,
    TRY_CAST("4" AS BIGINT) AS m4,
    TRY_CAST("5" AS BIGINT) AS m5,
    TRY_CAST("6" AS BIGINT) AS m6,
    TRY_CAST("7" AS BIGINT) AS m7,
    TRY_CAST("8" AS BIGINT) AS m8,
    TRY_CAST("9" AS BIGINT) AS m9,
    TRY_CAST("10" AS BIGINT) AS m10,
    TRY_CAST("11" AS BIGINT) AS m11,
    TRY_CAST("12" AS BIGINT) AS m12
FROM creative_budget;

-- Step 2: Unpivot months into rows
-- Use UNION ALL instead of UNPIVOT to preserve all rows
CREATE OR REPLACE TABLE budget_monthly AS
SELECT "請求年", "カテゴリ", 1 AS "請求月", m1 AS "売上予算額" FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 2, m2 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 3, m3 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 4, m4 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 5, m5 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 6, m6 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 7, m7 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 8, m8 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 9, m9 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 10, m10 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 11, m11 FROM budget_cleaned
UNION ALL SELECT "請求年", "カテゴリ", 12, m12 FROM budget_cleaned;

-- Step 3: Create 請求日 from year+month, add 請求日（期）
CREATE OR REPLACE TABLE budget_with_date AS
SELECT 
    *,
    TRY_CAST(CONCAT(CAST("請求年" AS VARCHAR), '-', LPAD(CAST("請求月" AS VARCHAR), 2, '0'), '-01') AS DATE) AS "請求日_raw"
FROM budget_monthly;

-- Add 9-hour offset like DOMO does, then calc period
CREATE OR REPLACE TABLE budget_with_period AS
SELECT 
    *,
    "請求日_raw" AS "請求日",
    CASE 
        WHEN "請求日_raw" IS NULL THEN '請求日未入力'
        WHEN FLOOR((YEAR("請求日_raw") - 2024) * 2 + ((MONTH("請求日_raw") - 1) / 6)) = 
             FLOOR((YEAR((SELECT ref_date FROM pipeline_config)) - 2024) * 2 + ((MONTH((SELECT ref_date FROM pipeline_config)) - 1) / 6))
            THEN '今期'
        WHEN FLOOR((YEAR("請求日_raw") - 2024) * 2 + ((MONTH("請求日_raw") - 1) / 6)) = 
             FLOOR((YEAR((SELECT ref_date FROM pipeline_config)) - 2024) * 2 + ((MONTH((SELECT ref_date FROM pipeline_config)) - 1) / 6)) - 1
            THEN '前期'
        ELSE CONCAT(
            CAST(FLOOR((YEAR("請求日_raw") - 2024) * 2 + ((MONTH("請求日_raw") - 1) / 6) + 36) AS VARCHAR),
            '期'
        )
    END AS "請求日（期）"
FROM budget_with_date;

-- Step 4: Cast types
CREATE OR REPLACE TABLE budget_typed AS
SELECT 
    "請求年", "カテゴリ", "請求月",
    CAST("売上予算額" AS BIGINT) AS "売上予算額",
    CAST("請求日" AS DATE) AS "請求日",
    "請求日（期）"
FROM budget_with_period;

-- Step 5: Cumulative budget by period + category
CREATE OR REPLACE TABLE budget_cumulative AS
SELECT 
    *,
    SUM("売上予算額") OVER (
        PARTITION BY "請求日（期）", "カテゴリ"
        ORDER BY "請求日" ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "累計売上予算額"
FROM budget_typed;

-- Step 6: Add BLカテゴリ = '予算'
CREATE OR REPLACE TABLE budget_final AS
SELECT 
    *,
    '予算' AS "BLカテゴリ"
FROM budget_cumulative;

-- ============================================================
-- HISTORICAL DATA BRANCH (DC課_粗利過去データ)
-- ============================================================

-- Step 1: Rename columns
CREATE OR REPLACE TABLE history_renamed AS
SELECT
    "_COLUMN_1" AS "年",
    TRY_CAST("1月" AS BIGINT) AS m1,
    TRY_CAST("2月" AS BIGINT) AS m2,
    TRY_CAST("3月" AS BIGINT) AS m3,
    TRY_CAST("4月" AS BIGINT) AS m4,
    TRY_CAST("5月" AS BIGINT) AS m5,
    TRY_CAST("6月" AS BIGINT) AS m6,
    TRY_CAST("7月" AS BIGINT) AS m7,
    TRY_CAST("8月" AS BIGINT) AS m8,
    TRY_CAST("9月" AS BIGINT) AS m9,
    TRY_CAST("10月" AS BIGINT) AS m10,
    TRY_CAST("11月" AS BIGINT) AS m11,
    TRY_CAST("12月" AS BIGINT) AS m12
FROM dc_history;

-- Step 2: Unpivot months (use UNION ALL to preserve all rows)
CREATE OR REPLACE TABLE history_monthly AS
SELECT "年", 1 AS "月", m1 AS "税抜費用（int）" FROM history_renamed
UNION ALL SELECT "年", 2, m2 FROM history_renamed
UNION ALL SELECT "年", 3, m3 FROM history_renamed
UNION ALL SELECT "年", 4, m4 FROM history_renamed
UNION ALL SELECT "年", 5, m5 FROM history_renamed
UNION ALL SELECT "年", 6, m6 FROM history_renamed
UNION ALL SELECT "年", 7, m7 FROM history_renamed
UNION ALL SELECT "年", 8, m8 FROM history_renamed
UNION ALL SELECT "年", 9, m9 FROM history_renamed
UNION ALL SELECT "年", 10, m10 FROM history_renamed
UNION ALL SELECT "年", 11, m11 FROM history_renamed
UNION ALL SELECT "年", 12, m12 FROM history_renamed;

-- Step 3: Create 請求日 + add constants
CREATE OR REPLACE TABLE history_with_date AS
SELECT 
    "年",
    "月",
    "税抜費用（int）",
    TRY_CAST(CONCAT("年", '-', LPAD(CAST("月" AS VARCHAR), 2, '0'), '-01') AS DATE) AS "請求日",
    YEAR(TRY_CAST(CONCAT("年", '-', LPAD(CAST("月" AS VARCHAR), 2, '0'), '-01') AS DATE)) AS "請求年",
    'PTJ加盟店サイト関連' AS "プロジェクト名",
    CAST(1073851282 AS BIGINT) AS "プロジェクトID",
    CAST(999999999 AS BIGINT) AS "課題ID",
    '完了' AS "ステータス名",
    '過去粗利データ' AS "課題タイトル",
    '課題リスト' AS "BLカテゴリ",
    CAST(0 AS BIGINT) AS "税抜外注費用"
FROM history_monthly;

-- Step 4: Filter to <= 2024-12-31
CREATE OR REPLACE TABLE history_final AS
SELECT * FROM history_with_date
WHERE "請求日" <= DATE '2024-12-31';
