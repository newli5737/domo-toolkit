-- 02_projects_cleaned.sql
-- Clean projects list: keep only id, projectKey, name
-- Maps to: SelectValues (不要列を削除)

CREATE OR REPLACE TABLE projects_cleaned AS
SELECT 
    id,
    projectKey,
    name
FROM backlog_projects_list;
