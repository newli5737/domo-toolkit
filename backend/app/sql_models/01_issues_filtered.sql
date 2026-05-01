-- 01_issues_filtered.sql
-- Branch A: Backlog Issues processing
-- Maps to: Filter 2 → Metadata → Filter/Filter(NULL) → ExpressionEvaluator

-- Step 1: Filter issues with ERAWAN code (4-digit year prefix like "2024／...")
CREATE OR REPLACE TABLE issues_erawan AS
SELECT *
FROM backlog_issue_list
WHERE regexp_matches(issueType_name, '^\d{4}／');

-- Step 2: Cast customFields5_value to DATE (請求日)
CREATE OR REPLACE TABLE issues_with_date AS
SELECT *,
    TRY_CAST(customFields5_value AS DATE) AS billing_date_parsed
FROM issues_erawan;

-- Step 3a: Issues with billing date >= 2025-04-01
-- The dataflow filter says: customFields5_value >= '2025-04-01'
CREATE OR REPLACE TABLE issues_dated AS
SELECT *,
    CONCAT('<a href="https://mothers-sp.backlog.jp/view/', issueKey, 
           '" target="_blank">', issueKey, '</a>') AS "課題URL"
FROM issues_with_date
WHERE billing_date_parsed >= DATE '2025-04-01';

-- Step 3b: Issues with NULL billing date
CREATE OR REPLACE TABLE issues_null_date AS
SELECT *,
    CONCAT('<a href="https://mothers-sp.backlog.jp/view/', issueKey, 
           '" target="_blank">', issueKey, '</a>') AS "課題URL"
FROM issues_with_date
WHERE billing_date_parsed IS NULL;
