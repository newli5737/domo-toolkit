-- 12_final_output.sql
-- Final UNION ALL of 4 branches + LEFT JOIN child task count

-- Step 1: Count child tasks per parent issue
CREATE OR REPLACE TABLE child_task_count AS
SELECT 
    "親課題ID",
    COUNT("課題ID") AS "子課題数"
FROM all_reunioned
WHERE "親課題ID" IS NOT NULL
GROUP BY "親課題ID";

-- Step 2: UNION ALL 4 branches
-- All branches must have same columns:
-- Core: プロジェクト名, プロジェクトID, 親課題ID, 課題ID, ステータスID, ステータス名,
--        課題タイトル, タスクの担当者名, 加盟店名, 開始日, 期限日, 税抜費用,
--        担当者①〜⑨ + ウェイト, 課題URL, 請求日, 税抜外注費用, ERAWANコード,
--        課題の登録日, AP加盟店ID, 登録者, 親/子課題, 請求日（期）, 税抜費用（int）
-- Branch-specific: 累計売上額（見込み）, 累計売上額（実績）, BLカテゴリ, 請求年,
--        担当者種別, 担当者, 種, ウェイト, カテゴリ, 請求月, 売上予算額, 累計売上予算額, 年, 月

CREATE OR REPLACE TABLE final_union AS

-- Branch 1: 課題リスト (task list with cumulative sales)
SELECT 
    "プロジェクト名", "プロジェクトID", "親課題ID", "課題ID",
    "ステータスID", "ステータス名", "課題タイトル", "タスクの担当者名",
    "加盟店名", "開始日", "期限日", "税抜費用",
    "担当者①", "担当者①のウェイト", "担当者②", "担当者②のウェイト",
    "課題URL", "担当者③", "担当者③のウェイト", "担当者④", "担当者④のウェイト",
    "担当者⑤", "担当者⑤のウェイト", "請求日", "税抜外注費用",
    "ERAWANコード", "課題の登録日", "担当者⑨のウェイト",
    "担当者⑧", "担当者⑧のウェイト", "担当者⑨",
    "担当者⑦のウェイト", "担当者⑦", "担当者⑥のウェイト", "担当者⑥",
    CAST("AP加盟店ID" AS VARCHAR) AS "AP加盟店ID", "登録者",
    "親/子課題", "請求日（期）", "税抜費用（int）",
    "累計売上額（見込み）", "累計売上額（実績）",
    "BLカテゴリ", "請求年",
    NULL::VARCHAR AS "担当者種別",
    NULL::VARCHAR AS "担当者",
    NULL::VARCHAR AS "種",
    NULL::BIGINT AS "ウェイト",
    NULL::VARCHAR AS "カテゴリ",
    NULL::BIGINT AS "請求月",
    NULL::BIGINT AS "売上予算額",
    NULL::BIGINT AS "累計売上予算額",
    NULL::VARCHAR AS "年",
    NULL::BIGINT AS "月"
FROM task_list_branch

UNION ALL BY NAME

-- Branch 2: ウェイト別課題 (weight pivot)
SELECT 
    "プロジェクト名", "プロジェクトID", "親課題ID", "課題ID",
    "ステータスID", "ステータス名", "課題タイトル", "タスクの担当者名",
    "加盟店名", "開始日", "期限日", "税抜費用", "税抜費用（int）",
    "課題URL", "請求日", "税抜外注費用", "ERAWANコード", "課題の登録日",
    CAST("AP加盟店ID" AS VARCHAR) AS "AP加盟店ID", "登録者",
    "親/子課題", "請求日（期）",
    "担当者種別", "担当者", "種", "ウェイト",
    "BLカテゴリ", "請求年",
    NULL::VARCHAR AS "担当者①", NULL::BIGINT AS "担当者①のウェイト",
    NULL::VARCHAR AS "担当者②", NULL::BIGINT AS "担当者②のウェイト",
    NULL::VARCHAR AS "担当者③", NULL::BIGINT AS "担当者③のウェイト",
    NULL::VARCHAR AS "担当者④", NULL::BIGINT AS "担当者④のウェイト",
    NULL::VARCHAR AS "担当者⑤", NULL::BIGINT AS "担当者⑤のウェイト",
    NULL::VARCHAR AS "担当者⑥", NULL::BIGINT AS "担当者⑥のウェイト",
    NULL::VARCHAR AS "担当者⑦", NULL::BIGINT AS "担当者⑦のウェイト",
    NULL::VARCHAR AS "担当者⑧", NULL::BIGINT AS "担当者⑧のウェイト",
    NULL::VARCHAR AS "担当者⑨", NULL::BIGINT AS "担当者⑨のウェイト",
    NULL::BIGINT AS "累計売上額（見込み）",
    NULL::BIGINT AS "累計売上額（実績）",
    NULL::VARCHAR AS "カテゴリ",
    NULL::BIGINT AS "請求月",
    NULL::BIGINT AS "売上予算額",
    NULL::BIGINT AS "累計売上予算額",
    NULL::VARCHAR AS "年",
    NULL::BIGINT AS "月"
FROM weight_pivot_joined

UNION ALL BY NAME

-- Branch 3: 予算 (budget)
SELECT 
    "BLカテゴリ", "請求年",
    CAST("請求日" AS DATE) AS "請求日",
    "請求日（期）",
    "カテゴリ",
    "請求月",
    "売上予算額",
    "累計売上予算額",
    NULL::VARCHAR AS "プロジェクト名", NULL::BIGINT AS "プロジェクトID",
    NULL::BIGINT AS "親課題ID", NULL::BIGINT AS "課題ID",
    NULL::VARCHAR AS "ステータスID", NULL::VARCHAR AS "ステータス名",
    NULL::VARCHAR AS "課題タイトル", NULL::VARCHAR AS "タスクの担当者名",
    NULL::VARCHAR AS "加盟店名", NULL::DATE AS "開始日", NULL::DATE AS "期限日",
    NULL::VARCHAR AS "税抜費用", NULL::BIGINT AS "税抜費用（int）",
    NULL::VARCHAR AS "課題URL", NULL::BIGINT AS "税抜外注費用",
    NULL::VARCHAR AS "ERAWANコード", NULL::DATE AS "課題の登録日",
    NULL::VARCHAR AS "AP加盟店ID", NULL::VARCHAR AS "登録者",
    NULL::VARCHAR AS "親/子課題",
    NULL::VARCHAR AS "担当者種別", NULL::VARCHAR AS "担当者", 
    NULL::VARCHAR AS "種", NULL::BIGINT AS "ウェイト",
    NULL::VARCHAR AS "担当者①", NULL::BIGINT AS "担当者①のウェイト",
    NULL::VARCHAR AS "担当者②", NULL::BIGINT AS "担当者②のウェイト",
    NULL::VARCHAR AS "担当者③", NULL::BIGINT AS "担当者③のウェイト",
    NULL::VARCHAR AS "担当者④", NULL::BIGINT AS "担当者④のウェイト",
    NULL::VARCHAR AS "担当者⑤", NULL::BIGINT AS "担当者⑤のウェイト",
    NULL::VARCHAR AS "担当者⑥", NULL::BIGINT AS "担当者⑥のウェイト",
    NULL::VARCHAR AS "担当者⑦", NULL::BIGINT AS "担当者⑦のウェイト",
    NULL::VARCHAR AS "担当者⑧", NULL::BIGINT AS "担当者⑧のウェイト",
    NULL::VARCHAR AS "担当者⑨", NULL::BIGINT AS "担当者⑨のウェイト",
    NULL::BIGINT AS "累計売上額（見込み）",
    NULL::BIGINT AS "累計売上額（実績）",
    NULL::VARCHAR AS "年",
    NULL::BIGINT AS "月"
FROM budget_final

UNION ALL BY NAME

-- Branch 4: 過去粗利 (historical data)
SELECT 
    "プロジェクト名", "プロジェクトID", "課題ID",
    "ステータス名", "課題タイトル", 
    CAST("請求日" AS DATE) AS "請求日", "請求年",
    "BLカテゴリ", "税抜外注費用", "税抜費用（int）",
    "年", "月",
    NULL::BIGINT AS "親課題ID",
    NULL::VARCHAR AS "ステータスID",
    NULL::VARCHAR AS "タスクの担当者名",
    NULL::VARCHAR AS "加盟店名", NULL::DATE AS "開始日", NULL::DATE AS "期限日",
    NULL::VARCHAR AS "税抜費用",
    NULL::VARCHAR AS "課題URL", NULL::VARCHAR AS "ERAWANコード",
    NULL::DATE AS "課題の登録日",
    NULL::VARCHAR AS "AP加盟店ID", NULL::VARCHAR AS "登録者",
    NULL::VARCHAR AS "親/子課題", NULL::VARCHAR AS "請求日（期）",
    NULL::VARCHAR AS "担当者種別", NULL::VARCHAR AS "担当者", 
    NULL::VARCHAR AS "種", NULL::BIGINT AS "ウェイト",
    NULL::VARCHAR AS "担当者①", NULL::BIGINT AS "担当者①のウェイト",
    NULL::VARCHAR AS "担当者②", NULL::BIGINT AS "担当者②のウェイト",
    NULL::VARCHAR AS "担当者③", NULL::BIGINT AS "担当者③のウェイト",
    NULL::VARCHAR AS "担当者④", NULL::BIGINT AS "担当者④のウェイト",
    NULL::VARCHAR AS "担当者⑤", NULL::BIGINT AS "担当者⑤のウェイト",
    NULL::VARCHAR AS "担当者⑥", NULL::BIGINT AS "担当者⑥のウェイト",
    NULL::VARCHAR AS "担当者⑦", NULL::BIGINT AS "担当者⑦のウェイト",
    NULL::VARCHAR AS "担当者⑧", NULL::BIGINT AS "担当者⑧のウェイト",
    NULL::VARCHAR AS "担当者⑨", NULL::BIGINT AS "担当者⑨のウェイト",
    NULL::BIGINT AS "累計売上額（見込み）",
    NULL::BIGINT AS "累計売上額（実績）",
    NULL::VARCHAR AS "カテゴリ",
    NULL::BIGINT AS "請求月",
    NULL::BIGINT AS "売上予算額",
    NULL::BIGINT AS "累計売上予算額"
FROM history_final;

-- Step 3: LEFT JOIN with child task count
CREATE OR REPLACE TABLE final_output AS
SELECT 
    f.*,
    c."子課題数"
FROM final_union f
LEFT JOIN child_task_count c ON f."課題ID" = c."親課題ID";
