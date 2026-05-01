-- 10_weight_pivot.sql
-- 担当者 pivot: 9 columns → rows with weight
-- IMPORTANT: DOMO keeps ALL rows including NULL担当者 (113k NULL rows)
-- Maps to: Filter(保守/CB分岐) → UnionAll → Normalizer → NormalizeAll → Filter(match) → Metadata

-- The DOMO Normalizer generates 1 row per 担当者 column per task (9 rows per task)
-- Then NormalizeAll generates 1 row per weight column per above row (9 × 9 = 81 rows per task)
-- Then Filter keeps only where 種 matches 担当者種別 (back to 9 rows per task)
-- NULL担当者 rows are KEPT - this is why DOMO has 115k rows

-- Step 1: Generate 9 rows per task by manually creating the unpivot
-- (DuckDB UNPIVOT drops NULLs, but DOMO keeps them)
CREATE OR REPLACE TABLE tantousha_expanded AS
SELECT *, '担当者①' AS "担当者種別", "担当者①" AS "担当者" FROM all_reunioned
UNION ALL
SELECT *, '担当者②', "担当者②" FROM all_reunioned
UNION ALL
SELECT *, '担当者③', "担当者③" FROM all_reunioned
UNION ALL
SELECT *, '担当者④', "担当者④" FROM all_reunioned
UNION ALL
SELECT *, '担当者⑤', "担当者⑤" FROM all_reunioned
UNION ALL
SELECT *, '担当者⑥', "担当者⑥" FROM all_reunioned
UNION ALL
SELECT *, '担当者⑦', "担当者⑦" FROM all_reunioned
UNION ALL
SELECT *, '担当者⑧', "担当者⑧" FROM all_reunioned
UNION ALL
SELECT *, '担当者⑨', "担当者⑨" FROM all_reunioned;

-- Step 2: Match weight column to 担当者種別
-- 担当者① → 担当者①のウェイト, etc.
CREATE OR REPLACE TABLE weight_pivot_joined AS
SELECT 
    "プロジェクト名", "プロジェクトID", "親課題ID", "課題ID",
    "ステータスID", "ステータス名", "課題タイトル", "加盟店名",
    "開始日", "期限日", "税抜費用", "税抜費用（int）",
    "親/子課題", "課題URL", "請求日", "タスクの担当者名",
    "請求日（期）", "税抜外注費用", "ERAWANコード", "課題の登録日",
    "AP加盟店ID", "登録者",
    "担当者種別",
    "担当者",
    -- Match weight by 担当者種別
    CASE "担当者種別"
        WHEN '担当者①' THEN "担当者①のウェイト"
        WHEN '担当者②' THEN "担当者②のウェイト"
        WHEN '担当者③' THEN "担当者③のウェイト"
        WHEN '担当者④' THEN "担当者④のウェイト"
        WHEN '担当者⑤' THEN "担当者⑤のウェイト"
        WHEN '担当者⑥' THEN "担当者⑥のウェイト"
        WHEN '担当者⑦' THEN "担当者⑦のウェイト"
        WHEN '担当者⑧' THEN "担当者⑧のウェイト"
        WHEN '担当者⑨' THEN "担当者⑨のウェイト"
    END AS "ウェイト",
    -- 種 column (weight column name) - matches DOMO output
    CASE "担当者種別"
        WHEN '担当者①' THEN '担当者①のウェイト'
        WHEN '担当者②' THEN '担当者②のウェイト'
        WHEN '担当者③' THEN '担当者③のウェイト'
        WHEN '担当者④' THEN '担当者④のウェイト'
        WHEN '担当者⑤' THEN '担当者⑤のウェイト'
        WHEN '担当者⑥' THEN '担当者⑥のウェイト'
        WHEN '担当者⑦' THEN '担当者⑦のウェイト'
        WHEN '担当者⑧' THEN '担当者⑧のウェイト'
        WHEN '担当者⑨' THEN '担当者⑨のウェイト'
    END AS "種",
    'ウェイト別課題' AS "BLカテゴリ",
    YEAR("請求日") AS "請求年"
FROM tantousha_expanded;
