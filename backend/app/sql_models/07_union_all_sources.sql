-- 07_union_all_sources.sql
-- Union 5 sources + LEFT JOIN with Projects
-- Maps to: UnionAll(行を追加) → MergeJoin(プロジェクト名を結合) → Filter(PTj) → Metadata(日本語表記)

-- Step 1: UNION ALL from all 5 branches
-- Normalize columns: each branch needs same schema for union
CREATE OR REPLACE TABLE all_sources_union AS

-- Branch A1: Issues with date
SELECT 
    CAST(id AS BIGINT) AS id,
    CAST(projectId AS BIGINT) AS projectId,
    TRY_CAST(startDate AS DATE) AS startDate,
    TRY_CAST(dueDate AS DATE) AS dueDate,
    summary,
    status_id,
    status_name,
    assignee_name,
    issueType_name,
    customFields0_value,
    CAST(customFields2_value AS VARCHAR) AS customFields2_value,
    TRY_CAST(customFields3_value AS BIGINT) AS customFields3_value,
    TRY_CAST(customFields5_value AS VARCHAR) AS customFields5_value_str,
    billing_date_parsed AS customFields5_value,
    customFields6_value_name,
    TRY_CAST(customFields7_value AS BIGINT) AS customFields7_value,
    customFields8_value_name AS customFields8_value_name,
    TRY_CAST(customFields9_value AS BIGINT) AS customFields9_value,
    customFields10_value_name,
    TRY_CAST(customFields11_value AS BIGINT) AS customFields11_value,
    customFields12_value_name,
    TRY_CAST(customFields13_value AS BIGINT) AS customFields13_value,
    customFields14_value_name,
    TRY_CAST(customFields15_value AS BIGINT) AS customFields15_value,
    CAST(customFields1_value AS VARCHAR) AS customFields1_value,
    TRY_CAST(parentIssueId AS BIGINT) AS parentIssueId,
    TRY_CAST(created AS DATE) AS created,
    createdUser_name,
    "課題URL",
    -- These columns don't exist in issues, fill NULL
    NULL AS "担当者６",
    NULL AS "担当者６ウェイト",
    NULL AS "担当者７",
    NULL AS "担当者７ウェイト",
    NULL AS "担当者８",
    NULL AS "担当者８ウェイト",
    NULL AS "担当者９",
    NULL AS "担当者９ウェイト"
FROM issues_dated

UNION ALL

-- Branch A2: Issues with NULL date
SELECT 
    CAST(id AS BIGINT),
    CAST(projectId AS BIGINT),
    TRY_CAST(startDate AS DATE),
    TRY_CAST(dueDate AS DATE),
    summary,
    status_id,
    status_name,
    assignee_name,
    issueType_name,
    customFields0_value,
    CAST(customFields2_value AS VARCHAR),
    TRY_CAST(customFields3_value AS BIGINT),
    TRY_CAST(customFields5_value AS VARCHAR),
    billing_date_parsed,
    customFields6_value_name,
    TRY_CAST(customFields7_value AS BIGINT),
    customFields8_value_name,
    TRY_CAST(customFields9_value AS BIGINT),
    customFields10_value_name,
    TRY_CAST(customFields11_value AS BIGINT),
    customFields12_value_name,
    TRY_CAST(customFields13_value AS BIGINT),
    customFields14_value_name,
    TRY_CAST(customFields15_value AS BIGINT),
    CAST(customFields1_value AS VARCHAR),
    TRY_CAST(parentIssueId AS BIGINT),
    TRY_CAST(created AS DATE),
    createdUser_name,
    "課題URL",
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM issues_null_date

UNION ALL

-- Branch B: Excel import
SELECT 
    CAST(id AS BIGINT),
    CAST(projectId AS BIGINT),
    TRY_CAST(startDate AS DATE),
    TRY_CAST(dueDate AS DATE),
    summary,
    NULL AS status_id,
    status_name,
    NULL AS assignee_name,
    issueType_name,
    customFields0_value,
    CAST(customFields2_value AS VARCHAR),
    TRY_CAST(customFields3_value AS BIGINT),
    NULL,
    customFields5_value,
    customFields6_value_name,
    customFields7_value,
    NULL AS customFields8_value_name,
    TRY_CAST(customFields8_value AS BIGINT),
    customFields10_value_name,
    customFields11_value,
    customFields12_value_name,
    customFields13_value,
    customFields14_value_name,
    customFields15_value,
    CAST(customFields1_value AS VARCHAR),
    NULL AS parentIssueId,
    NULL AS created,
    createdUser_name,
    "課題URL",
    "担当者６",
    "担当者６ウェイト",
    "担当者７",
    "担当者７ウェイト",
    "担当者８",
    "担当者８ウェイト",
    "担当者９",
    "担当者９ウェイト"
FROM excel_filtered

UNION ALL

-- Branch C: Monthly fees (expanded)
SELECT 
    CAST(id AS BIGINT),
    CAST(projectId AS BIGINT),
    startDate,
    dueDate,
    summary,
    NULL AS status_id,
    status_name,
    NULL AS assignee_name,
    CAST(issueType_name AS VARCHAR),
    customFields0_value,
    CAST(customFields2_value AS VARCHAR),
    NULL AS customFields3_value,
    NULL,
    customFields5_value,
    NULL AS customFields6_value_name,
    CAST(customFields7_value AS BIGINT),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL AS customFields1_value,
    NULL AS parentIssueId,
    NULL AS created,
    NULL AS createdUser_name,
    NULL AS "課題URL",
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM monthly_fee_final

UNION ALL

-- Branch D: Subscription LP (expanded)
SELECT 
    CAST(id AS BIGINT),
    CAST(projectId AS BIGINT),
    startDate,
    dueDate,
    summary,
    NULL AS status_id,
    status_name,
    NULL AS assignee_name,
    CAST(issueType_name AS VARCHAR),
    customFields0_value,
    CAST(customFields2_value AS VARCHAR),
    NULL AS customFields3_value,
    NULL,
    customFields5_value,
    NULL AS customFields6_value_name,
    CAST(customFields7_value AS BIGINT),
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL AS customFields1_value,
    NULL AS parentIssueId,
    NULL AS created,
    NULL AS createdUser_name,
    NULL AS "課題URL",
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM sub_lp_final;

-- Step 2: LEFT JOIN with Projects to get project name
CREATE OR REPLACE TABLE with_project_name AS
SELECT 
    u.*,
    p.name AS project_name
FROM all_sources_union u
LEFT JOIN projects_cleaned p ON u.projectId = p.id;

-- Step 3: Filter to PTj project only (projectId = 1073851282)
CREATE OR REPLACE TABLE ptj_filtered AS
SELECT * FROM with_project_name
WHERE projectId = 1073851282;
