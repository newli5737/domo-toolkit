-- 09_cost_cumulative.sql
-- Cost type branching + cumulative sales calculations
-- Maps to: Filter(テキスト/数値/NULL) → Metadata(int cast) → Constant → SetValueField → UnionAll
--          → WindowAction(累計見込み) → Filter(完了) → WindowAction(累計実績) → MergeJoin
--          → Constant(BLカテゴリ) → ExpressionEvaluator(請求年)

-- Step 1: Split by cost type and create 税抜費用（int）

-- Numeric costs: cast to integer
CREATE OR REPLACE TABLE cost_numeric AS
SELECT *,
    TRY_CAST("税抜費用_cleaned" AS BIGINT) AS "税抜費用（int）"
FROM post_processed
WHERE "税抜費用_cleaned" != ''
  AND "税抜費用_cleaned" IS NOT NULL
  AND NOT regexp_matches("税抜費用_cleaned", '[^0-9]');

-- Text costs (non-numeric): keep as-is, int = NULL
CREATE OR REPLACE TABLE cost_text AS
SELECT *,
    NULL::BIGINT AS "税抜費用（int）"
FROM post_processed
WHERE "税抜費用_cleaned" IS NOT NULL
  AND "税抜費用_cleaned" != ''
  AND regexp_matches("税抜費用_cleaned", '[^0-9]');

-- NULL costs
CREATE OR REPLACE TABLE cost_null AS
SELECT *,
    NULL::BIGINT AS "税抜費用（int）"
FROM post_processed
WHERE "税抜費用_cleaned" IS NULL OR "税抜費用_cleaned" = '';

-- Step 2: Union all cost branches back together
CREATE OR REPLACE TABLE all_reunioned AS
SELECT * FROM cost_text
UNION ALL
SELECT * FROM cost_numeric
UNION ALL
SELECT * FROM cost_null;

-- Step 3: Cumulative sales (見込み) — all rows
CREATE OR REPLACE TABLE cumulative_forecast AS
SELECT *,
    SUM("税抜費用（int）") OVER (
        PARTITION BY "請求日（期）"
        ORDER BY "請求日" ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "累計売上額（見込み）"
FROM all_reunioned;

-- Step 4: Cumulative sales (実績) — only completed/billed tasks
CREATE OR REPLACE TABLE cumulative_actual AS
SELECT 
    "請求日",
    "課題ID",
    "課題タイトル",
    SUM("税抜費用（int）") OVER (
        PARTITION BY "請求日（期）"
        ORDER BY "請求日" ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS "累計売上額（実績）"
FROM all_reunioned
WHERE "ステータス名" IN ('完了', '請求済', '定期更新')
  AND "請求日" IS NOT NULL;

-- Step 5: LEFT JOIN forecast with actual (on 請求日 + 課題ID + 課題タイトル)
CREATE OR REPLACE TABLE with_cumulative AS
SELECT 
    f.*,
    a."累計売上額（実績）"
FROM cumulative_forecast f
LEFT JOIN cumulative_actual a
    ON f."請求日" IS NOT DISTINCT FROM a."請求日"
   AND f."課題ID" IS NOT DISTINCT FROM a."課題ID"
   AND f."課題タイトル" IS NOT DISTINCT FROM a."課題タイトル";

-- Step 6: Add BLカテゴリ = '課題リスト' and 請求年
CREATE OR REPLACE TABLE task_list_branch AS
SELECT 
    *,
    '課題リスト' AS "BLカテゴリ",
    YEAR("請求日") AS "請求年"
FROM with_cumulative;
