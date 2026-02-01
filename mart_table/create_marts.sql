-- =================================================================
-- 1. CONFORMED DIMENSIONS
-- =================================================================

-- 1.1 DIM_DATE (Standard Calendar)
-- In production, this would be loaded from a static reference table.
CREATE TABLE dim_Date AS
SELECT
    CAST(DATE_FORMAT(CalendarDate, 'yyyyMMdd') AS INT) as DateKey,
    CalendarDate as DateValue,
    YEAR(CalendarDate) as Year,
    MONTH(CalendarDate) as Month,
    DATE_FORMAT(CalendarDate, 'MMM') as MonthName,
    QUARTER(CalendarDate) as Quarter,
    DAYOFWEEK(CalendarDate) as DayOfWeek,
    CASE WHEN DAYOFWEEK(CalendarDate) IN (1, 7) THEN 1 ELSE 0 END as IsWeekend
FROM Reference_Date_Source; -- Placeholder

-- 1.2 DIM_PERSON (Consolidated User List)
-- Combines Owners, Contributors, and Modifiers into one master directory.
CREATE TABLE dim_Person AS
WITH All_Users AS (
    SELECT UserId, Name, Email, 'Owner' as Source FROM src_Owners
    UNION ALL
    SELECT UserId, Name, Email, 'Contributor' as Source FROM src_Contributors
    UNION ALL
    SELECT ModifiedBy_UserId, ModifiedBy_Name, ModifiedBy_Email, 'Modifier' as Source 
    FROM src_Issues WHERE ModifiedBy_UserId IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER(ORDER BY Email) as PersonKey, -- Surrogate Key
    UserId as SourceSystemId,
    Name,
    Email,
    -- Handle logic if a user appears multiple times (prioritize first role found)
    FIRST(Source) as PrimaryRoleSource 
FROM All_Users
GROUP BY UserId, Name, Email;

-- 1.3 DIM_DEPARTMENT
CREATE TABLE dim_Department AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY DepartmentName) as DepartmentKey,
    DepartmentTypeId as SourceDepartmentId,
    DepartmentName
FROM src_Departments
GROUP BY DepartmentTypeId, DepartmentName;

-- 1.4 DIM_TAG
CREATE TABLE dim_Tag AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY Value) as TagKey,
    Value as TagName
FROM src_Tags
GROUP BY Value;


-- =================================================================
-- 2. FACT TABLES
-- =================================================================

-- 2.1 FCT_ISSUES (Transactional Fact)
-- Grain: One row per Issue
CREATE TABLE fct_Issues AS
WITH Normalized_Issues AS (
    SELECT 
        IssueId,
        
        -- DATA CLEANING: Sanitize Title
        -- Replace Em Dash (—) and En Dash (–) with standard hyphen (-) to fix encoding errors in CSV/Excel
        regexp_replace(Title, '[—–]', '-') as Title,
        
        Type,
        IsExternalIssue,
        ImpactsCustomer,
        Details,
        ModifiedBy_UserId,
        CreatedAt,
        DateOccurred,
        DateIdentified,
        
        -- ROBUST DATE PARSING LOGIC (Data Quality)
        -- Handles mixed formats: ISO Strings vs 13-digit Epoch Milliseconds
        CASE 
            WHEN CreatedAt RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(CreatedAt AS LONG))
            ELSE to_timestamp(CreatedAt) 
        END as Norm_CreatedAt,
        
        CASE 
            WHEN DateOccurred RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(DateOccurred AS LONG))
            ELSE to_timestamp(DateOccurred) 
        END as Norm_DateOccurred,

        CASE 
            WHEN DateIdentified RLIKE '^[0-9]{13}$' THEN timestamp_millis(CAST(DateIdentified AS LONG))
            ELSE to_timestamp(DateIdentified) 
        END as Norm_DateIdentified
        
    FROM src_Issues
)
SELECT 
    ROW_NUMBER() OVER(ORDER BY i.IssueId) as IssueKey,
    i.IssueId as NaturalKey,
    
    -- Foreign Keys to Dimensions (Smart Date Keys)
    -- We map NULLs to 19000101 to maintain Referential Integrity
    COALESCE(CAST(DATE_FORMAT(i.Norm_CreatedAt, 'yyyyMMdd') AS INT), 19000101) as CreatedDateKey,
    COALESCE(CAST(DATE_FORMAT(i.Norm_DateOccurred, 'yyyyMMdd') AS INT), 19000101) as OccurredDateKey,
    COALESCE(CAST(DATE_FORMAT(i.Norm_DateIdentified, 'yyyyMMdd') AS INT), 19000101) as IdentifiedDateKey,
    
    -- Link to Modifier (Person Dimension)
    p.PersonKey as ModifiedByPersonKey,
    
    -- Degenerate Dimensions (Context that doesn't need a separate join)
    Title,
    Type as IssueType,
    Details,
    IsExternalIssue,
    ImpactsCustomer,
    
    -- Measures / Metrics
    DATEDIFF(day, Norm_DateOccurred, Norm_DateIdentified) as DaysToIdentify_Lag,
    DATEDIFF(day, Norm_CreatedAt, CURRENT_DATE()) as AgeDays
    
FROM Normalized_Issues i
LEFT JOIN dim_Person p ON i.ModifiedBy_UserId = p.SourceUserId;

-- 2.2 FCT_ISSUE_ATTRIBUTES (EAV Table for Custom Data)
-- Allows analysis of dynamic fields without altering table schema
CREATE TABLE fct_Issue_Attributes AS
SELECT
    f.IssueKey,
    ca.AttributeKey,
    ca.AttributeValue
FROM src_CustomAttributes ca
JOIN fct_Issues f ON ca.IssueId = f.NaturalKey;


-- =================================================================
-- 3. BRIDGE TABLES (Handling Many-to-Many Arrays)
-- =================================================================

-- 3.1 BRIDGE: ISSUES <-> OWNERS/CONTRIBUTORS
-- We combine both roles into one bridge for cleaner modeling
CREATE TABLE bridge_Issue_People AS
SELECT 
    f.IssueKey,
    p.PersonKey,
    'Owner' as RoleType
FROM src_Owners s
JOIN fct_Issues f ON s.IssueId = f.NaturalKey
JOIN dim_Person p ON s.UserId = p.SourceSystemId
UNION ALL
SELECT 
    f.IssueKey,
    p.PersonKey,
    'Contributor' as RoleType
FROM src_Contributors s
JOIN fct_Issues f ON s.IssueId = f.NaturalKey
JOIN dim_Person p ON s.UserId = p.SourceSystemId;

-- 3.2 BRIDGE: ISSUES <-> DEPARTMENTS
CREATE TABLE bridge_Issue_Departments AS
SELECT 
    f.IssueKey,
    d.DepartmentKey
FROM src_Departments s
JOIN fct_Issues f ON s.IssueId = f.NaturalKey
JOIN dim_Department d ON s.DepartmentTypeId = d.SourceDepartmentId;

-- 3.3 BRIDGE: ISSUES <-> TAGS
CREATE TABLE bridge_Issue_Tags AS
SELECT 
    f.IssueKey,
    t.TagKey
FROM src_Tags s
JOIN fct_Issues f ON s.IssueId = f.NaturalKey
JOIN dim_Tag t ON s.Value = t.TagName;