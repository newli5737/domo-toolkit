-- 06_excel_filtered.sql
-- Branch B: DC課_Domo取り込み1,2月案件.xlsx
-- Filter: summary NOT LIKE '%保守費%' AND summary NOT LIKE '%チャットボット月額%'

CREATE OR REPLACE TABLE excel_filtered AS
SELECT 
    CAST(id AS BIGINT) AS id,
    CAST(projectId AS BIGINT) AS projectId,
    startDate,
    dueDate,
    customFields0_value,
    customFields6_value_name,
    TRY_CAST(customFields7_value AS BIGINT) AS customFields7_value,
    customFields9_value_name,
    TRY_CAST(customFields8_value AS BIGINT) AS customFields8_value,
    customFields10_value_name,
    TRY_CAST(customFields11_value AS BIGINT) AS customFields11_value,
    customFields12_value_name,
    TRY_CAST(customFields13_value AS BIGINT) AS customFields13_value,
    customFields14_value_name,
    TRY_CAST(customFields15_value AS BIGINT) AS customFields15_value,
    "担当者６",
    TRY_CAST("担当者６ウェイト" AS BIGINT) AS "担当者６ウェイト",
    "担当者７",
    TRY_CAST("担当者７ウェイト" AS BIGINT) AS "担当者７ウェイト",
    "担当者８",
    TRY_CAST("担当者８ウェイト" AS BIGINT) AS "担当者８ウェイト",
    "担当者９",
    TRY_CAST("担当者９ウェイト" AS BIGINT) AS "担当者９ウェイト",
    "課題URL",
    TRY_CAST(customFields5_value AS DATE) AS customFields5_value,
    CAST(customFields2_value AS VARCHAR) AS customFields2_value,
    summary,
    status_name,
    issueType_name,
    NULL::VARCHAR AS customFields1_value,
    NULL::BIGINT AS customFields3_value,
    TRY_CAST(created AS DATE) AS created,
    NULL::VARCHAR AS createdUser_name
FROM dc_excel_import
WHERE (summary NOT LIKE '%保守費%' OR summary IS NULL)
  AND (summary NOT LIKE '%チャットボット月額%' OR summary IS NULL);
