-- 08_post_union_processing.sql
-- Rename columns to Japanese + add 親/子課題, clean 税抜費用, calc 請求日（期）

-- Step 1: Rename all columns to Japanese business names
CREATE OR REPLACE TABLE renamed_japanese AS
SELECT 
    project_name AS "プロジェクト名",
    CAST(projectId AS BIGINT) AS "プロジェクトID",
    parentIssueId AS "親課題ID",
    CAST(id AS BIGINT) AS "課題ID",
    status_id AS "ステータスID",
    status_name AS "ステータス名",
    summary AS "課題タイトル",
    assignee_name AS "タスクの担当者名",
    customFields0_value AS "加盟店名",
    TRY_CAST(startDate AS DATE) AS "開始日",
    TRY_CAST(dueDate AS DATE) AS "期限日",
    customFields2_value AS "税抜費用",
    customFields6_value_name AS "担当者①",
    customFields7_value AS "担当者①のウェイト",
    customFields8_value_name AS "担当者②",
    customFields9_value AS "担当者②のウェイト",
    "課題URL",
    customFields10_value_name AS "担当者③",
    customFields11_value AS "担当者③のウェイト",
    customFields12_value_name AS "担当者④",
    customFields13_value AS "担当者④のウェイト",
    customFields14_value_name AS "担当者⑤",
    customFields15_value AS "担当者⑤のウェイト",
    customFields5_value AS "請求日",
    customFields3_value AS "税抜外注費用",
    issueType_name AS "ERAWANコード",
    created AS "課題の登録日",
    "担当者６" AS "担当者⑥",
    TRY_CAST("担当者６ウェイト" AS BIGINT) AS "担当者⑥のウェイト",
    "担当者７" AS "担当者⑦",
    TRY_CAST("担当者７ウェイト" AS BIGINT) AS "担当者⑦のウェイト",
    "担当者８" AS "担当者⑧",
    TRY_CAST("担当者８ウェイト" AS BIGINT) AS "担当者⑧のウェイト",
    "担当者９" AS "担当者⑨",
    TRY_CAST("担当者９ウェイト" AS BIGINT) AS "担当者⑨のウェイト",
    customFields1_value AS "AP加盟店ID",
    createdUser_name AS "登録者"
FROM ptj_filtered;

-- Step 2: Add computed columns
-- IMPORTANT: 請求日（期）uses FLOOR() which produces float (38.0期), matching DOMO output
CREATE OR REPLACE TABLE post_processed AS
SELECT 
    *,
    -- 親/子課題 flag
    CASE WHEN "親課題ID" IS NOT NULL THEN '子課題' ELSE '親課題' END AS "親/子課題",
    -- Clean 税抜費用: remove commas
    REPLACE(COALESCE(CAST("税抜費用" AS VARCHAR), ''), ',', '') AS "税抜費用_cleaned",
    -- 請求日（期）: billing period calculation
    -- Uses FLOOR() which produces float output (38.0期) matching DOMO
    CASE 
        WHEN "請求日" IS NULL THEN '請求日未入力'
        WHEN FLOOR((YEAR("請求日") - 2024) * 2 + ((MONTH("請求日") - 1) / 6)) = 
             FLOOR((YEAR((SELECT ref_date FROM pipeline_config)) - 2024) * 2 + ((MONTH((SELECT ref_date FROM pipeline_config)) - 1) / 6))
            THEN '今期'
        WHEN FLOOR((YEAR("請求日") - 2024) * 2 + ((MONTH("請求日") - 1) / 6)) = 
             FLOOR((YEAR((SELECT ref_date FROM pipeline_config)) - 2024) * 2 + ((MONTH((SELECT ref_date FROM pipeline_config)) - 1) / 6)) - 1
            THEN '前期'
        ELSE CONCAT(
            CAST(FLOOR((YEAR("請求日") - 2024) * 2 + ((MONTH("請求日") - 1) / 6) + 36) AS VARCHAR),
            '期'
        )
    END AS "請求日（期）"
FROM renamed_japanese;
