-- 03_calendar_prepared.sql
-- Calendar dimension: filter year>=2024, day=1, create proper date column
-- Maps to: Filter 2024~ → Metadata → ExpressionEvaluator (YearMonth)

-- Note: calendar_date in source is just the day number (VARCHAR: '01','02',...,'31')
-- Need to construct full date from year + month + date

-- Step 1: Filter calendar to year >= 2024 and day = 1 (first of month only)
CREATE OR REPLACE TABLE calendar_filtered AS
SELECT *,
    -- Construct actual date from year + month + day
    TRY_CAST(
        CONCAT(
            CAST(calendar_year AS VARCHAR), '-',
            LPAD(CAST(calendar_month AS VARCHAR), 2, '0'), '-',
            LPAD(CAST(calendar_date AS VARCHAR), 2, '0')
        ) AS DATE
    ) AS actual_date
FROM er_calendar
WHERE calendar_year >= 2024
  AND CAST(calendar_date AS VARCHAR) = '01';

-- Step 2: Create calendar_prepared with proper date
CREATE OR REPLACE TABLE calendar_prepared AS
SELECT 
    calendar_year,
    calendar_month,
    actual_date AS calendar_date,
    CAST(calendar_month AS INTEGER) AS calendar_month_int,
    CONCAT(CAST(calendar_year AS VARCHAR), '-', LPAD(CAST(calendar_month AS VARCHAR), 2, '0')) AS year_month
FROM calendar_filtered;
