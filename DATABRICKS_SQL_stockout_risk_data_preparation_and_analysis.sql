01 — Material Master Data Validation

SELECT * FROM material_master_raw;

SELECT COUNT(*)
FROM material_master_raw;

SELECT COUNT (DISTINCT material_ID)
FROM material_master_raw;


02 — Customer Master Data Validation
SELECT * FROM customer_master_raw;

SELECT COUNT (*)
FROM customer_master_raw;

SELECT COUNT (DISTINCT Customer_ID)
FROM customer_master_raw;
--Table name: workspace.default.customer_master (Created by the file upload UI), columns = [`Customer_ID`:


03 — Demand Actuals Data Validation
--Preview demand actuals
SELECT * FROM demand_actuals_raw;

--Check total row count
SELECT COUNT (*)
FROM demand_actuals_raw;

--Identify duplicate Month + Customer + Material combinations
SELECT Demand_Month, Customer_ID, Material_ID,
COUNT (*) AS Row_Count
FROM demand_actuals_raw
GROUP BY
Demand_Month, Customer_ID, Material_ID
HAVING COUNT (*) > 1;

--Inspection of sample duplicates
SELECT * FROM demand_actuals_raw
WHERE Demand_Month = '2026-05-01'
AND Customer_ID = 'C072'
AND Material_ID = 'MAT-0025';

--Validate if duplicate records contain the same demand value
SELECT Demand_Month, Customer_ID, Material_ID,
COUNT (*) AS Row_Count,
COUNT (DISTINCT Actual_Demand_Units)
AS Distinct_Demand_Values
FROM demand_actuals_raw
GROUP BY
Demand_Month, Customer_ID, Material_ID 
HAVING COUNT (*) > 1;

--Check required fields for missing values
SELECT * FROM demand_actuals_raw
WHERE
Demand_Month IS NULL
OR Customer_ID IS NULL
OR Material_ID IS NULL
OR Actual_Demand_Units IS NULL;

--Check invalid demand values
SELECT * FROM demand_actuals_raw
WHERE Actual_Demand_Units < 0;

--Check material ID from Demand actuals raw exists in Material Master raw
SELECT demand_actuals_raw.Material_ID,material_master_raw.Material_ID
FROM demand_actuals_raw
LEFT JOIN material_master_raw
ON demand_actuals_raw.Material_ID = material_master_raw.Material_ID
WHERE material_master_raw.Material_ID IS NULL;

--Check customer ID from Demand actuals raw exists in Customer Master raw
SELECT demand_actuals_raw.Customer_ID,customer_master_raw.Customer_ID
FROM demand_actuals_raw
LEFT JOIN customer_master_raw
ON demand_actuals_raw.Customer_ID = customer_Master_raw.Customer_ID
WHERE customer_master_raw.Customer_ID IS NULL;

--Check months coverage historical demand data cover
SELECT DISTINCT Demand_Month
FROM demand_actuals_raw
ORDER BY Demand_Month ASC;

SELECT
MIN (Demand_Month) AS Earliest_Month,
MAX (Demand_Month) AS Latest_Month
FROM
demand_actuals_raw;

--Count unique Customer x Material combinations 
SELECT COUNT (DISTINCT Customer_ID, Material_ID)
FROM demand_actuals_raw;
--May 500 material IDs pero represents 898 unique Customer x Material demand relationships;
--898 combinations × 24 months = 21,552 expected
--Actual total rows = 21,577
--Rows diff = 25
--May 25 dupes ng customer x material x month records

--Cross checking if 898 combinations have a complete 24-month history
SELECT Customer_ID, Material_ID,
COUNT (DISTINCT Demand_Month)
AS Distinct_Demand_Month
FROM demand_actuals_raw
00


--Each combination may exactly 24 unique demand months — Sep 2024 to Aug 2026
SELECT Customer_ID, Material_ID,
COUNT(DISTINCT Demand_Month) AS Distinct_Demand_Month
FROM demand_actuals_raw
GROUP BY Customer_ID, Material_ID
HAVING COUNT(DISTINCT Demand_Month) <> 24;

--Only unique rows from the raw demand actuals
SELECT DISTINCT COUNT (*)
FROM (SELECT DISTINCT * FROM demand_actuals_raw) 
AS unique_demand;

--Creating new table using result of netong query
CREATE TABLE demand_actuals_clean 
AS
SELECT DISTINCT * FROM demand_actuals_raw;

--Checking new demand actuals clean table
SELECT * FROM demand_actuals_clean

SELECT COUNT (*) FROM demand_actuals_clean


04 — Demand Fcst Data Validation
SELECT *
FROM demand_forecast_raw;

SELECT COUNT (*)
FROM demand_forecast_raw;

SELECT Forecast_Month, Customer_ID, Material_ID,
COUNT(*) AS Row_Count
FROM demand_forecast_raw
GROUP BY Forecast_Month, Customer_ID, Material_ID
HAVING Row_Count > 1;
--Walang duplicate records were found for the Forecast Month × Customer × Material combination.


-- Check NULL values in critical forecast key fields

SELECT *
FROM demand_forecast_raw
WHERE Forecast_Month IS NULL OR Customer_ID IS NULL
OR Material_ID IS NULL;


--Check forecast horizon

SELECT MIN (Forecast_Month) AS Earliest_Forecast_Month,
MAX (Forecast_Month) AS Latest_Forecast_Month
FROM demand_forecast_raw;


-- Check forecast Material IDs exist in Material Master / Customer IDs exist in Customer master 

SELECT demand_forecast_raw.Material_ID, material_master_raw.Material_ID
FROM demand_forecast_raw
LEFT JOIN material_master_raw
ON demand_forecast_raw.Material_ID = material_master_raw.Material_ID
WHERE material_master_raw.Material_ID IS NULL;


SELECT demand_forecast_raw.Customer_ID, customer_master_raw.Customer_ID
FROM demand_forecast_raw
LEFT JOIN customer_master_raw
ON demand_forecast_raw.Customer_ID = customer_master_raw.Customer_ID
WHERE customer_master_raw.Customer_ID IS NULL;



05 — Inventory Snapshot Data Validation
SELECT *
FROM inventory_snapshot_raw;

--Check duplicate records for Snapshot date x Materila ID x Site id combination.
SELECT Snapshot_Date, Material_ID, Site_ID,
COUNT(*) AS Row_Count
FROM inventory_snapshot_raw
GROUP BY Snapshot_Date, Material_ID, Site_ID
HAVING Row_Count > 1;

-- Check NULL values in critical inventory fields
SELECT * FROM inventory_snapshot_raw
WHERE Snapshot_Date IS NULL
OR Material_ID IS NULL
OR Site_ID IS NULL
OR On_Hand_Units IS NULL
OR Available_Inventory_Units IS NULL;

-- Check inventory snapshot date coverage
SELECT
MIN(Snapshot_Date) AS Earliest_Snapshot,
MAX(Snapshot_Date) AS Latest_Snapshot
FROM inventory_snapshot_raw;


-- Check invalid negative inventory values
SELECT *
FROM inventory_snapshot_raw
WHERE On_Hand_Units < 0
OR Quality_Hold_Units < 0
OR Available_Inventory_Units < 0
OR Safety_Stock_Units < 0;

-- Check available inventory does not exceed on-hand
SELECT *
FROM inventory_snapshot_raw
WHERE Available_Inventory_Units > On_Hand_Units;


-- Check inventory Material IDs exist in Material Master

SELECT inventory_snapshot_raw.Material_ID,
material_master_raw.Material_ID
FROM inventory_snapshot_raw
LEFT JOIN material_master_raw
ON inventory_snapshot_raw.Material_ID = material_master_raw.Material_ID
WHERE material_master_raw.Material_ID IS NULL;


06 — Planned Supply Data Validation
SELECT *
FROM planned_supply_raw;

-- Check total planned supply records
SELECT COUNT(*) AS Row_Count
FROM planned_supply_raw;

-- Check duplicate Supply IDs
SELECT Supply_ID,
COUNT(*) AS Row_Count
FROM planned_supply_raw
GROUP BY Supply_ID
HAVING COUNT(*) > 1;


-- Check NULL values in critical planned supply fields
SELECT *
FROM planned_supply_raw
WHERE Supply_ID IS NULL
OR Material_ID IS NULL
OR Site_ID IS NULL
OR Receipt_Month IS NULL
OR Planned_Supply_Units IS NULL
OR Supply_Status IS NULL
OR Supply_Type IS NULL;
--may mga nag null

-- Identify which fields contain NULL values (formula assisted gpt) formula means NULL? = 1; NOT NULL? = 0; SUM() = all 1
SELECT
    COUNT(*) AS Total_Rows,
    SUM(CASE WHEN Supply_ID IS NULL THEN 1 ELSE 0 END) AS Null_Supply_ID,
    SUM(CASE WHEN Material_ID IS NULL THEN 1 ELSE 0 END) AS Null_Material_ID,
    SUM(CASE WHEN Site_ID IS NULL THEN 1 ELSE 0 END) AS Null_Site_ID,
    SUM(CASE WHEN Receipt_Month IS NULL THEN 1 ELSE 0 END) AS Null_Receipt_Month,
    SUM(CASE WHEN Planned_Supply_Units IS NULL THEN 1 ELSE 0 END) AS Null_Supply_Units,
    SUM(CASE WHEN Supply_Status IS NULL THEN 1 ELSE 0 END) AS Null_Supply_Status,
    SUM(CASE WHEN Supply_Type IS NULL THEN 1 ELSE 0 END) AS Null_Supply_Type
FROM planned_supply_raw;


-- Review supply status distribution
SELECT Supply_Status,
COUNT(*) AS Row_Count,
SUM(Planned_Supply_Units) AS Supply_Units
FROM planned_supply_raw
GROUP BY Supply_Status
ORDER BY Row_Count DESC;


-- Check planned supply horizon
SELECT MIN(Receipt_Month) AS Earliest_Receipt_Month,
MAX(Receipt_Month) AS Latest_Receipt_Month
FROM planned_supply_raw;


-- Check invalid negative planned supply
SELECT *
FROM planned_supply_raw
WHERE Planned_Supply_Units < 0;


-- Check planned supply Material IDs exist in Material Master
SELECT planned_supply_raw.Material_ID,
material_master_raw.Material_ID
FROM planned_supply_raw
LEFT JOIN material_master_raw
ON planned_supply_raw.Material_ID = material_master_raw.Material_ID
WHERE material_master_raw.Material_ID IS NULL;



--Historical Actuals → Aug 2026
-- Inventory snapshot → Aug 31, 2026
--Forecast → Sep 2026–Feb 2027
--Planned Supply → Sep 2026–Feb 2027


07 — Stockout Risk Analysis
-- Check whether materials are stocked across multiple sites

SELECT Material_ID,
COUNT(DISTINCT Site_ID) AS Site_Count
FROM inventory_snapshot_raw
GROUP BY Material_ID
HAVING COUNT(DISTINCT Site_ID) > 1
ORDER BY Site_Count DESC;


-- Aggregate customer forecasts to Material x Month

SELECT Forecast_Month, Material_ID,
SUM(Current_Forecast_Units) AS Total_Forecast_Units
FROM demand_forecast_raw
GROUP BY Forecast_Month, Material_ID
ORDER BY Material_ID, Forecast_Month;


-- Aggregate planned supply to Material x Month by status (gpt assisted foumla)

SELECT
    Receipt_Month,
    Material_ID,
    SUM(CASE WHEN Supply_Status = 'Confirmed'
        THEN Planned_Supply_Units ELSE 0 END) AS Confirmed_Supply_Units,
    SUM(CASE WHEN Supply_Status = 'Planned'
        THEN Planned_Supply_Units ELSE 0 END) AS Planned_Supply_Units,
    SUM(CASE WHEN Supply_Status = 'At Risk'
        THEN Planned_Supply_Units ELSE 0 END) AS At_Risk_Supply_Units,
    SUM(CASE WHEN Supply_Status IS NULL
        THEN Planned_Supply_Units ELSE 0 END) AS Unknown_Supply_Units
FROM planned_supply_raw
GROUP BY
    Receipt_Month,
    Material_ID
ORDER BY
    Material_ID,
    Receipt_Month;


-- Combine monthly forecast and planned supply (gpt assisted formuls)

WITH forecast AS (
    SELECT
        Forecast_Month AS Month,
        Material_ID,
        SUM(Current_Forecast_Units) AS Forecast_Units
    FROM demand_forecast_raw
    GROUP BY Forecast_Month, Material_ID
),

supply AS (
    SELECT
        Receipt_Month AS Month,
        Material_ID,
        SUM(CASE WHEN Supply_Status = 'Confirmed'
            THEN Planned_Supply_Units ELSE 0 END) AS Confirmed_Supply,
        SUM(CASE WHEN Supply_Status = 'Planned'
            THEN Planned_Supply_Units ELSE 0 END) AS Planned_Supply,
        SUM(CASE WHEN Supply_Status = 'At Risk'
            THEN Planned_Supply_Units ELSE 0 END) AS At_Risk_Supply,
        SUM(CASE WHEN Supply_Status IS NULL
            THEN Planned_Supply_Units ELSE 0 END) AS Unknown_Supply
    FROM planned_supply_raw
    GROUP BY Receipt_Month, Material_ID
)

SELECT
    f.Month,
    f.Material_ID,
    f.Forecast_Units,
    COALESCE(s.Confirmed_Supply, 0) AS Confirmed_Supply,
    COALESCE(s.Planned_Supply, 0) AS Planned_Supply,
    COALESCE(s.At_Risk_Supply, 0) AS At_Risk_Supply,
    COALESCE(s.Unknown_Supply, 0) AS Unknown_Supply
FROM forecast f
LEFT JOIN supply s
    ON f.Month = s.Month
   AND f.Material_ID = s.Material_ID
ORDER BY
    f.Material_ID,
    f.Month;


-- Project inventory using confirmed supply only (gpt assisted formula)

WITH forecast AS (
    SELECT
        Forecast_Month AS Month,
        Material_ID,
        SUM(Current_Forecast_Units) AS Forecast_Units
    FROM demand_forecast_raw
    GROUP BY Forecast_Month, Material_ID
),

supply AS (
    SELECT
        Receipt_Month AS Month,
        Material_ID,
        SUM(CASE WHEN Supply_Status = 'Confirmed'
            THEN Planned_Supply_Units ELSE 0 END) AS Confirmed_Supply,
        SUM(CASE WHEN Supply_Status = 'Planned'
            THEN Planned_Supply_Units ELSE 0 END) AS Planned_Supply,
        SUM(CASE WHEN Supply_Status = 'At Risk'
            THEN Planned_Supply_Units ELSE 0 END) AS At_Risk_Supply,
        SUM(CASE WHEN Supply_Status IS NULL
            THEN Planned_Supply_Units ELSE 0 END) AS Unknown_Supply
    FROM planned_supply_raw
    GROUP BY Receipt_Month, Material_ID
),

monthly_flow AS (
    SELECT
        f.Month,
        f.Material_ID,
        i.Site_ID,
        i.Available_Inventory_Units AS Starting_Inventory,
        i.Safety_Stock_Units,
        f.Forecast_Units,
        COALESCE(s.Confirmed_Supply, 0) AS Confirmed_Supply,
        COALESCE(s.Planned_Supply, 0) AS Planned_Supply,
        COALESCE(s.At_Risk_Supply, 0) AS At_Risk_Supply,
        COALESCE(s.Unknown_Supply, 0) AS Unknown_Supply
    FROM forecast f
    LEFT JOIN supply s
        ON f.Month = s.Month
       AND f.Material_ID = s.Material_ID
    LEFT JOIN inventory_snapshot_raw i
        ON f.Material_ID = i.Material_ID
)

SELECT
    *,
    Starting_Inventory
    + SUM(Confirmed_Supply - Forecast_Units)
        OVER (
            PARTITION BY Material_ID
            ORDER BY Month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS Projected_Inventory
FROM monthly_flow
ORDER BY Material_ID, Month;


-- Count materials with projected stockout risk
-- Confirmed supply only (gpt assisted formulas)

WITH forecast AS (
    SELECT
        Forecast_Month AS Month,
        Material_ID,
        SUM(Current_Forecast_Units) AS Forecast_Units
    FROM demand_forecast_raw
    GROUP BY Forecast_Month, Material_ID
),

supply AS (
    SELECT
        Receipt_Month AS Month,
        Material_ID,
        SUM(CASE WHEN Supply_Status = 'Confirmed'
            THEN Planned_Supply_Units ELSE 0 END) AS Confirmed_Supply
    FROM planned_supply_raw
    GROUP BY Receipt_Month, Material_ID
),

monthly_flow AS (
    SELECT
        f.Month,
        f.Material_ID,
        i.Available_Inventory_Units AS Starting_Inventory,
        f.Forecast_Units,
        COALESCE(s.Confirmed_Supply, 0) AS Confirmed_Supply
    FROM forecast f
    LEFT JOIN supply s
        ON f.Month = s.Month
       AND f.Material_ID = s.Material_ID
    LEFT JOIN inventory_snapshot_raw i
        ON f.Material_ID = i.Material_ID
),

projection AS (
    SELECT
        Month,
        Material_ID,
        Starting_Inventory,
        Forecast_Units,
        Confirmed_Supply,
        Starting_Inventory
        + SUM(Confirmed_Supply - Forecast_Units)
            OVER (
                PARTITION BY Material_ID
                ORDER BY Month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS Projected_Inventory
    FROM monthly_flow
)

SELECT
    COUNT(DISTINCT Material_ID) AS Materials_With_Stockout_Risk
FROM projection
WHERE Projected_Inventory < 0;

--showed na there's 414 distinct materials are projected to fall below zero inventory at least once between Sep 2026 and Feb 2027.


-- Create analytical dataset for stockout risk analysis (gpt assisted formuls)

CREATE OR REPLACE TABLE stockout_risk_analysis AS

WITH forecast AS (
    SELECT
        Forecast_Month AS Month,
        Material_ID,
        SUM(Current_Forecast_Units) AS Forecast_Units
    FROM demand_forecast_raw
    GROUP BY Forecast_Month, Material_ID
),

supply AS (
    SELECT
        Receipt_Month AS Month,
        Material_ID,
        SUM(CASE WHEN Supply_Status = 'Confirmed'
            THEN Planned_Supply_Units ELSE 0 END) AS Confirmed_Supply,
        SUM(CASE WHEN Supply_Status = 'Planned'
            THEN Planned_Supply_Units ELSE 0 END) AS Planned_Supply,
        SUM(CASE WHEN Supply_Status = 'At Risk'
            THEN Planned_Supply_Units ELSE 0 END) AS At_Risk_Supply,
        SUM(CASE WHEN Supply_Status IS NULL
            THEN Planned_Supply_Units ELSE 0 END) AS Unknown_Supply
    FROM planned_supply_raw
    GROUP BY Receipt_Month, Material_ID
),

monthly_flow AS (
    SELECT
        f.Month,
        f.Material_ID,
        i.Site_ID,
        i.Available_Inventory_Units AS Starting_Inventory,
        i.Safety_Stock_Units,
        f.Forecast_Units,
        COALESCE(s.Confirmed_Supply, 0) AS Confirmed_Supply,
        COALESCE(s.Planned_Supply, 0) AS Planned_Supply,
        COALESCE(s.At_Risk_Supply, 0) AS At_Risk_Supply,
        COALESCE(s.Unknown_Supply, 0) AS Unknown_Supply
    FROM forecast f
    LEFT JOIN supply s
        ON f.Month = s.Month
       AND f.Material_ID = s.Material_ID
    LEFT JOIN inventory_snapshot_raw i
        ON f.Material_ID = i.Material_ID
),

projection AS (
    SELECT
        *,
        Starting_Inventory
        + SUM(Confirmed_Supply - Forecast_Units)
            OVER (
                PARTITION BY Material_ID
                ORDER BY Month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS Projected_Inventory
    FROM monthly_flow
)

SELECT
    *,
    CASE
        WHEN Projected_Inventory < 0 THEN 1
        ELSE 0
    END AS Stockout_Risk_Flag
FROM projection;

----

SELECT *
FROM stockout_risk_analysis
ORDER BY Material_ID, Month;

-----
SELECT COUNT(DISTINCT Material_ID) AS Stockout_Risk_Materials
FROM stockout_risk_analysis
WHERE Stockout_Risk_Flag = 1;