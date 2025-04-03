-- All the operations are on 'construction_lead_data' database.
USE construction_lead_data;

---------------------------------------------------------Environmental Department---------------------------------------------------------
-- Temporary table to list the broker group median premium for environmental department.
CREATE TEMP TABLE temp_env_median AS
SELECT DISTINCT
    product_department_name,
    branch,
    broker_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quoted_premium) 
        OVER (PARTITION BY product_department_name, branch, broker_group) AS broker_group_median_premium 
    ---FROM public.submission_data
    FROM public.submission_datamart
WHERE product_department_name = 'Environmental'
AND lob IN ('Contractors Pollution Liability XS', 'Contractors Pollution Liability (CPL)', 'Contractors Professional and Pollution Liability (',
                'Contractors Professional and Pollution Liab XS')
AND policy_status IN ('Policy Bound', 'Cancelled')
AND policy_source = 'DRAGON';

-- Temporary table to list submission count, bound count, broker group binding ratio with respect to the broker groups in atlanta and chicago region
CREATE TEMP TABLE temp_env_metrics AS
SELECT 
    product_department_name,
    branch,
    broker_group,
    COUNT(*) AS submission_count,
    COUNT(CASE WHEN policy_status IN ('Policy Bound', 'Cancelled') THEN 1 END) AS bound_count,
    CASE 
        WHEN submission_count != 0 THEN (bound_count::DECIMAL / submission_count * 100)::DECIMAL(4,1)  -- Check to prevent division by zero
        ELSE NULL
    END AS broker_group_binding_ratio -- Count for multiple values
--FROM public.submission_data
FROM public.submission_datamart
WHERE product_department_name = 'Environmental'-- Single condition for product_department_name
  AND lob IN ('Contractors Pollution Liability XS', 'Contractors Pollution Liability (CPL)', 'Contractors Professional and Pollution Liability (',
                'Contractors Professional and Pollution Liab XS') -- Multiple values for lob
  AND policy_source = 'DRAGON'
GROUP BY product_department_name, branch, broker_group
ORDER BY submission_count DESC;

-- drop table temp_env_metrics;

--Left joining the median table with the broker group binding ratio table for Environmental department.
CREATE TEMP TABLE temp_env_rule12 AS
SELECT 
    sda.product_department_name,
    sda.branch,
    sda.broker_group,
    sda.SUBMISSION_COUNT,
    sda.BOUND_COUNT,
    sda.BROKER_GROUP_BINDING_RATIO,
    COALESCE(md.broker_group_median_premium, 0) AS BROKER_GROUP_MEDIAN_PREMIUM
FROM temp_env_metrics sda
LEFT JOIN temp_env_median md
    ON sda.product_department_name = md.product_department_name
   AND sda.branch = md.branch
   AND sda.broker_group = md.broker_group
ORDER BY sda.SUBMISSION_COUNT DESC;

-- drop table temp_env_rule12;
-- Apply Total submissions per broker group must be at least 10 for the rules to apply​
-- select count(*) from temp_env_rule12;
-- CREATE TEMP TABLE temp_env_rule12_min AS
-- SELECT *
-- FROM temp_env_rule12
-- WHERE submission_count>=10;
-- select count(*) from temp_env_rule12_min;

-- UPDATE temp_env_rule12_min
-- SET broker_group_median_premium = 0
-- WHERE bound_count<2;

-- calculate the decile for broker group binding ratio and median value using qunatiles
ALTER TABLE temp_env_rule12
ADD COLUMN BROKER_GROUP_BINDING_RATIO_DECILE INTEGER;

ALTER TABLE temp_env_rule12
ADD COLUMN BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER;

WITH ranked_data AS (
    SELECT
        BROKER_GROUP_BINDING_RATIO,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_BINDING_RATIO) AS percentile_bindingratio,
        BROKER_GROUP_MEDIAN_PREMIUM,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_MEDIAN_PREMIUM) AS percentile_medianpremium,
        BROKER_GROUP_MEDIAN_PREMIUM,
        product_department_name,
        branch,
        broker_group
        FROM
        temp_env_rule12 where submission_count>=10 or bound_count >=2
),
computed_data AS (
    SELECT
        product_department_name,
        COALESCE(branch,'NULL') as branch,
        broker_group,
        CASE
            WHEN percentile_bindingratio <= 0.1 THEN 1
            WHEN percentile_bindingratio <= 0.2 THEN 2
            WHEN percentile_bindingratio <= 0.3 THEN 3
            WHEN percentile_bindingratio <= 0.4 THEN 4
            WHEN percentile_bindingratio <= 0.5 THEN 5
            WHEN percentile_bindingratio <= 0.6 THEN 6
            WHEN percentile_bindingratio <= 0.7 THEN 7
            WHEN percentile_bindingratio <= 0.8 THEN 8
            WHEN percentile_bindingratio <= 0.9 THEN 9
            ELSE 10
        END AS decile1,
        CASE
            WHEN percentile_medianpremium <= 0.1 THEN 1
            WHEN percentile_medianpremium <= 0.2 THEN 2
            WHEN percentile_medianpremium <= 0.3 THEN 3
            WHEN percentile_medianpremium <= 0.4 THEN 4
            WHEN percentile_medianpremium <= 0.5 THEN 5
            WHEN percentile_medianpremium <= 0.6 THEN 6
            WHEN percentile_medianpremium <= 0.7 THEN 7
            WHEN percentile_medianpremium <= 0.8 THEN 8
            WHEN percentile_medianpremium <= 0.9 THEN 9
            ELSE 10
        END AS decile2
    FROM ranked_data
)
UPDATE temp_env_rule12
SET BROKER_GROUP_BINDING_RATIO_DECILE = computed_data.decile1,
BROKER_GROUP_MEDIAN_PREMIUM_DECILE = computed_data.decile2
FROM computed_data
WHERE temp_env_rule12.product_department_name = computed_data.product_department_name
AND COALESCE(temp_env_rule12.branch,'NULL') = computed_data.branch
AND temp_env_rule12.broker_group = computed_data.broker_group;

select * from temp_env_rule12;
---------------------------------------------------------A&E Department---------------------------------------------------------

-- Temporary table to list the broker group median premium for A&E department.
CREATE TEMP TABLE temp_a_and_e_median AS
SELECT DISTINCT
    product_department_name,
    branch,
    broker_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quoted_premium) 
        OVER (PARTITION BY product_department_name, branch, broker_group) AS broker_group_median_premium 
    --FROM public.submission_data
    FROM public.submission_datamart
WHERE product_department_name = 'Environmental'
AND lob IN ('A&E Constructors Professional Primary', 'A&E Constructors Professional Excess', 'A&E Project Specific Excess', 'A&E Project Specific Primary',
                'Architects & Engineers Professional Primary', 'Architects & Engineers Professional Excess')
AND policy_status IN ('Policy Bound', 'Cancelled')
AND policy_source = 'DRAGON';

--drop table temp_a_and_e_median;

-- Temporary table to list submission count, bound count, broker group binding ratio with respect to the broker groups in atlanta and chicago region
CREATE TEMP TABLE temp_a_and_e_metrics AS
SELECT 
    product_department_name,
    branch,
    broker_group,
    COUNT(*) AS submission_count,
    COUNT(CASE WHEN policy_status IN ('Policy Bound', 'Cancelled') THEN 1 END) AS bound_count,
    CASE 
        WHEN submission_count != 0 THEN (bound_count::DECIMAL / submission_count * 100)::DECIMAL(4,1)  -- Check to prevent division by zero
        ELSE NULL
    END AS broker_group_binding_ratio -- Count for multiple values
--FROM public.submission_data
FROM public.submission_datamart
WHERE product_department_name = 'Environmental'-- Single condition for product_department_name
  AND lob IN ('A&E Constructors Professional Primary', 'A&E Constructors Professional Excess', 'A&E Project Specific Excess', 'A&E Project Specific Primary',
                'Architects & Engineers Professional Primary', 'Architects & Engineers Professional Excess') -- Multiple values for lob
  AND policy_source = 'DRAGON'
GROUP BY product_department_name, branch, broker_group
ORDER BY submission_count DESC;

-- drop table temp_a_and_e_metrics;

--Left joining the median table with the broker group binding ratio table for A&E Department
CREATE TEMP TABLE temp_a_and_e_rule12 AS
SELECT 
    sda.product_department_name,
    sda.branch,
    sda.broker_group,
    sda.SUBMISSION_COUNT,
    sda.BOUND_COUNT,
    sda.BROKER_GROUP_BINDING_RATIO,
    COALESCE(md.broker_group_median_premium, 0) AS BROKER_GROUP_MEDIAN_PREMIUM
FROM temp_a_and_e_metrics sda
LEFT JOIN temp_a_and_e_median md
    ON sda.product_department_name = md.product_department_name
   AND sda.branch = md.branch
   AND sda.broker_group = md.broker_group
ORDER BY sda.SUBMISSION_COUNT DESC;

-- drop table temp_a_and_e_rule12;
-- Apply Total submissions per broker group must be at least 10 for the rules to apply​
-- select count(*) from temp_a_and_e_rule12;
-- CREATE TEMP TABLE temp_a_and_e_rule12_min AS
-- SELECT *
-- FROM temp_a_and_e_rule12
-- WHERE submission_count>=10;
-- select count(*) from temp_a_and_e_rule12_min;

-- UPDATE temp_a_and_e_rule12_min
-- SET broker_group_median_premium = 0
-- WHERE bound_count<2;

-- calculate the decile for broker group binding ratio and median value using qunatiles
ALTER TABLE temp_a_and_e_rule12
ADD COLUMN BROKER_GROUP_BINDING_RATIO_DECILE INTEGER;

ALTER TABLE temp_a_and_e_rule12
ADD COLUMN BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER;

WITH ranked_data AS (
    SELECT
        BROKER_GROUP_BINDING_RATIO,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_BINDING_RATIO) AS percentile_bindingratio,
        BROKER_GROUP_MEDIAN_PREMIUM,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_MEDIAN_PREMIUM) AS percentile_medianpremium,
        BROKER_GROUP_MEDIAN_PREMIUM,
        product_department_name,
        branch,
        broker_group
        FROM
        temp_a_and_e_rule12
        where submission_count>=10 or bound_count >=2
),
computed_data AS (
    SELECT
        product_department_name,
        COALESCE(branch,'NULL') as branch,
        broker_group,
        CASE
            WHEN percentile_bindingratio <= 0.1 THEN 1
            WHEN percentile_bindingratio <= 0.2 THEN 2
            WHEN percentile_bindingratio <= 0.3 THEN 3
            WHEN percentile_bindingratio <= 0.4 THEN 4
            WHEN percentile_bindingratio <= 0.5 THEN 5
            WHEN percentile_bindingratio <= 0.6 THEN 6
            WHEN percentile_bindingratio <= 0.7 THEN 7
            WHEN percentile_bindingratio <= 0.8 THEN 8
            WHEN percentile_bindingratio <= 0.9 THEN 9
            ELSE 10
        END AS decile1,
        CASE
            WHEN percentile_medianpremium <= 0.1 THEN 1
            WHEN percentile_medianpremium <= 0.2 THEN 2
            WHEN percentile_medianpremium <= 0.3 THEN 3
            WHEN percentile_medianpremium <= 0.4 THEN 4
            WHEN percentile_medianpremium <= 0.5 THEN 5
            WHEN percentile_medianpremium <= 0.6 THEN 6
            WHEN percentile_medianpremium <= 0.7 THEN 7
            WHEN percentile_medianpremium <= 0.8 THEN 8
            WHEN percentile_medianpremium <= 0.9 THEN 9
            ELSE 10
        END AS decile2
    FROM ranked_data
)
UPDATE temp_a_and_e_rule12
SET BROKER_GROUP_BINDING_RATIO_DECILE = computed_data.decile1,
BROKER_GROUP_MEDIAN_PREMIUM_DECILE = computed_data.decile2
FROM computed_data
WHERE temp_a_and_e_rule12.product_department_name = computed_data.product_department_name
AND COALESCE(temp_a_and_e_rule12.branch,'NULL') = computed_data.branch
AND temp_a_and_e_rule12.broker_group = computed_data.broker_group;

select * from temp_a_and_e_rule12;

-- set product department name to A/E
UPDATE temp_a_and_e_rule12
SET product_department_name = 'A/E'
WHERE product_department_name = 'Environmental';
select * from temp_a_and_e_rule12;

---------------------------------------------------------Primary Construction Department---------------------------------------------------------

---- Temporary table to list the broker group median premium for Primary Construction department.
CREATE TEMP TABLE temp_pc_median AS
SELECT DISTINCT
    product_department_name,
    broker_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quoted_premium) 
        OVER (PARTITION BY product_department_name, broker_group) AS broker_group_median_premium
--FROM public.submission_data
FROM public.submission_datamart
WHERE product_department_name = 'Primary Construction'
  AND policy_status IN ('Cancelled','Expired','Forms Management - In Progress','Forms Management - User input required',
                           'In Force','In Force (Waiting for user to regenerate documents)','Rating - Suspended','Ready for Issuance',
                           'Ready for Issuance (Modifying Subject To)','Ready for Issuance (Waiting for user to regenerate documents)',
                           'Subject To - In Progress')
    -- AND (policy_source != 'DRAGON' or policy_source is null);
    AND (policy_source = 'MAJESCO');
-- Temporary table to list submission count, bound count, broker group binding ratio with respect to the broker groups in atlanta and chicago region
CREATE TEMP TABLE temp_pc_metrics AS
SELECT 
    product_department_name,
    NULL as branch,
    broker_group,
    COUNT(*) AS submission_count,
    COUNT(
        CASE WHEN policy_status IN ('Cancelled','Expired','Forms Management - In Progress','Forms Management - User input required',
                           'In Force','In Force (Waiting for user to regenerate documents)','Rating - Suspended','Ready for Issuance',
                           'Ready for Issuance (Modifying Subject To)','Ready for Issuance (Waiting for user to regenerate documents)',
                           'Subject To - In Progress')
            THEN 1 END) AS bound_count,
    CASE 
        WHEN submission_count != 0 THEN (bound_count::DECIMAL / submission_count * 100)::DECIMAL(4, 1)   -- Check to prevent division by zero
        ELSE NULL
    END AS broker_group_binding_ratio -- Count for multiple values
--FROM public.submission_data
FROM public.submission_datamart
WHERE product_department_name = 'Primary Construction'-- Single condition for product_department_name
-- AND (policy_source != 'DRAGON' or policy_source is null)
AND (policy_source = 'MAJESCO')
GROUP BY product_department_name, broker_group
ORDER BY submission_count DESC;


--Left joining the median table with the broker group binding ratio table for Primary Construction Department
CREATE TEMP TABLE temp_pc_rule12 AS
SELECT 
    sda.product_department_name,
    sda.branch,
    sda.broker_group,
    sda.SUBMISSION_COUNT,
    sda.BOUND_COUNT,
    sda.BROKER_GROUP_BINDING_RATIO,
    COALESCE(md.broker_group_median_premium, 0) AS BROKER_GROUP_MEDIAN_PREMIUM
FROM temp_pc_metrics sda
LEFT JOIN temp_pc_median md
    ON sda.product_department_name = md.product_department_name
    AND sda.broker_group = md.broker_group
ORDER BY sda.SUBMISSION_COUNT DESC;

-- Apply Total submissions per broker group must be at least 10 for the rules to apply​
-- select count(*) from temp_pc_rule12;
-- CREATE TEMP TABLE temp_pc_rule12_min AS
-- SELECT *
-- FROM temp_pc_rule12
-- WHERE submission_count>=10;
-- select count(*) from temp_pc_rule12_min;

-- UPDATE temp_pc_rule12_min
-- SET broker_group_median_premium = 0
-- WHERE bound_count<2;

-- calculate the decile for broker group binding ratio and median value using qunatiles
ALTER TABLE temp_pc_rule12
ADD COLUMN BROKER_GROUP_BINDING_RATIO_DECILE INTEGER;

ALTER TABLE temp_pc_rule12
ADD COLUMN BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER;

WITH ranked_data AS (
    SELECT
        BROKER_GROUP_BINDING_RATIO,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_BINDING_RATIO) AS percentile_bindingratio,
        BROKER_GROUP_MEDIAN_PREMIUM,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_MEDIAN_PREMIUM) AS percentile_medianpremium,
        BROKER_GROUP_MEDIAN_PREMIUM,
        product_department_name,
        branch,
        broker_group
        FROM
        temp_pc_rule12
        where submission_count>=10 or bound_count >=2

),
computed_data AS (
    SELECT
        product_department_name,
        COALESCE(branch,'NULL') as branch,
        broker_group,
        CASE
            WHEN percentile_bindingratio <= 0.1 THEN 1
            WHEN percentile_bindingratio <= 0.2 THEN 2
            WHEN percentile_bindingratio <= 0.3 THEN 3
            WHEN percentile_bindingratio <= 0.4 THEN 4
            WHEN percentile_bindingratio <= 0.5 THEN 5
            WHEN percentile_bindingratio <= 0.6 THEN 6
            WHEN percentile_bindingratio <= 0.7 THEN 7
            WHEN percentile_bindingratio <= 0.8 THEN 8
            WHEN percentile_bindingratio <= 0.9 THEN 9
            ELSE 10
        END AS decile1,
        CASE
            WHEN percentile_medianpremium <= 0.1 THEN 1
            WHEN percentile_medianpremium <= 0.2 THEN 2
            WHEN percentile_medianpremium <= 0.3 THEN 3
            WHEN percentile_medianpremium <= 0.4 THEN 4
            WHEN percentile_medianpremium <= 0.5 THEN 5
            WHEN percentile_medianpremium <= 0.6 THEN 6
            WHEN percentile_medianpremium <= 0.7 THEN 7
            WHEN percentile_medianpremium <= 0.8 THEN 8
            WHEN percentile_medianpremium <= 0.9 THEN 9
            ELSE 10
        END AS decile2
    FROM ranked_data
)
UPDATE temp_pc_rule12
SET BROKER_GROUP_BINDING_RATIO_DECILE = computed_data.decile1,
BROKER_GROUP_MEDIAN_PREMIUM_DECILE = computed_data.decile2
FROM computed_data
WHERE temp_pc_rule12.product_department_name = computed_data.product_department_name
AND COALESCE(temp_pc_rule12.branch,'NULL') = computed_data.branch
AND temp_pc_rule12.broker_group = computed_data.broker_group;

select * from temp_pc_rule12;

---------------------------------------------------------GMI Builders Risk---------------------------------------------------------
-- Temporary table to list the broker group median premium for Onshore construction department.
CREATE TEMP TABLE temp_onshore_construction_median AS
SELECT DISTINCT
    product_department_name,
    branch,
    broker_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quoted_premium) 
        OVER (PARTITION BY product_department_name, branch, broker_group) AS broker_group_median_premium 
    FROM public.submission_datamart
WHERE product_department_name = 'Onshore Construction'
AND lob IN ('Builders Risk')
AND policy_status IN ('Policy Bound', 'Cancelled')
AND policy_source = 'DRAGON';

-- Temporary table to list submission count, bound count, broker group binding ratio with respect to the broker groups
CREATE TEMP TABLE temp_onshore_construction_metrics AS
SELECT 
    product_department_name,
    branch,
    broker_group,
    COUNT(*) AS submission_count,
    COUNT(CASE WHEN policy_status IN ('Policy Bound', 'Cancelled') THEN 1 END) AS bound_count,
    CASE 
        WHEN submission_count != 0 THEN (bound_count::DECIMAL / submission_count * 100)::DECIMAL(4,1)  -- Check to prevent division by zero
        ELSE NULL
    END AS broker_group_binding_ratio 
FROM public.submission_datamart
WHERE product_department_name = 'Onshore Construction'
  AND lob IN ('Builders Risk')
  AND policy_source = 'DRAGON'
GROUP BY product_department_name, branch, broker_group
ORDER BY submission_count DESC;


--Left joining the median table with the broker group binding ratio table for onshore construction department.
CREATE TEMP TABLE temp_onshore_construction_rule12 AS
SELECT 
    sda.product_department_name,
    sda.branch,
    sda.broker_group,
    sda.SUBMISSION_COUNT,
    sda.BOUND_COUNT,
    sda.BROKER_GROUP_BINDING_RATIO,
    COALESCE(md.broker_group_median_premium, 0) AS BROKER_GROUP_MEDIAN_PREMIUM
FROM temp_onshore_construction_metrics sda
LEFT JOIN temp_onshore_construction_median md
    ON sda.product_department_name = md.product_department_name
   AND sda.branch = md.branch
   AND sda.broker_group = md.broker_group
ORDER BY sda.SUBMISSION_COUNT DESC;

-- calculate the decile for broker group binding ratio and median value using qunatiles
ALTER TABLE temp_onshore_construction_rule12
ADD COLUMN BROKER_GROUP_BINDING_RATIO_DECILE INTEGER;

ALTER TABLE temp_onshore_construction_rule12
ADD COLUMN BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER;

WITH ranked_data AS (
    SELECT
        BROKER_GROUP_BINDING_RATIO,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_BINDING_RATIO) AS percentile_bindingratio,
        BROKER_GROUP_MEDIAN_PREMIUM,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_MEDIAN_PREMIUM) AS percentile_medianpremium,
        BROKER_GROUP_MEDIAN_PREMIUM,
        product_department_name,
        branch,
        broker_group
        FROM
        temp_onshore_construction_rule12 where submission_count>=10 or bound_count >=2
),
computed_data AS (
    SELECT
        product_department_name,
        COALESCE(branch,'NULL') as branch,
        broker_group,
        CASE
            WHEN percentile_bindingratio <= 0.1 THEN 1
            WHEN percentile_bindingratio <= 0.2 THEN 2
            WHEN percentile_bindingratio <= 0.3 THEN 3
            WHEN percentile_bindingratio <= 0.4 THEN 4
            WHEN percentile_bindingratio <= 0.5 THEN 5
            WHEN percentile_bindingratio <= 0.6 THEN 6
            WHEN percentile_bindingratio <= 0.7 THEN 7
            WHEN percentile_bindingratio <= 0.8 THEN 8
            WHEN percentile_bindingratio <= 0.9 THEN 9
            ELSE 10
        END AS decile1,
        CASE
            WHEN percentile_medianpremium <= 0.1 THEN 1
            WHEN percentile_medianpremium <= 0.2 THEN 2
            WHEN percentile_medianpremium <= 0.3 THEN 3
            WHEN percentile_medianpremium <= 0.4 THEN 4
            WHEN percentile_medianpremium <= 0.5 THEN 5
            WHEN percentile_medianpremium <= 0.6 THEN 6
            WHEN percentile_medianpremium <= 0.7 THEN 7
            WHEN percentile_medianpremium <= 0.8 THEN 8
            WHEN percentile_medianpremium <= 0.9 THEN 9
            ELSE 10
        END AS decile2
    FROM ranked_data
)
UPDATE temp_onshore_construction_rule12
SET BROKER_GROUP_BINDING_RATIO_DECILE = computed_data.decile1,
BROKER_GROUP_MEDIAN_PREMIUM_DECILE = computed_data.decile2
FROM computed_data
WHERE temp_onshore_construction_rule12.product_department_name = computed_data.product_department_name
AND COALESCE(temp_onshore_construction_rule12.branch,'NULL') = computed_data.branch
AND temp_onshore_construction_rule12.broker_group = computed_data.broker_group;

select * from temp_onshore_construction_rule12;

---------------------------------------------------------Excess Casualty---------------------------------------------------------

CREATE TEMP TABLE temp_excess_casualty_median AS
SELECT DISTINCT
    product_department_name,
    branch,
    broker_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY quoted_premium) 
        OVER (PARTITION BY product_department_name, branch, broker_group) AS broker_group_median_premium 
    FROM public.submission_datamart
WHERE product_department_name = 'General Casualty'
AND lob IN ('General Liability')
AND policy_status IN ('Policy Bound', 'Cancelled')
AND policy_source = 'DRAGON';

-- Temporary table to list submission count, bound count, broker group binding ratio with respect to the broker groups
CREATE TEMP TABLE temp_excess_casualty_metrics AS
SELECT 
    product_department_name,
    branch,
    broker_group,
    COUNT(*) AS submission_count,
    COUNT(CASE WHEN policy_status IN ('Policy Bound', 'Cancelled') THEN 1 END) AS bound_count,
    CASE 
        WHEN submission_count != 0 THEN (bound_count::DECIMAL / submission_count * 100)::DECIMAL(4,1)  -- Check to prevent division by zero
        ELSE NULL
    END AS broker_group_binding_ratio 
FROM public.submission_datamart
WHERE product_department_name = 'General Casualty'
AND lob IN ('General Liability')
-- AND policy_status IN ('Policy Bound', 'Cancelled')
AND policy_source = 'DRAGON'
GROUP BY product_department_name, branch, broker_group
ORDER BY submission_count DESC;

--Left joining the median table with the broker group binding ratio table for excess casualty department.

CREATE TEMP TABLE temp_excess_casualty_rule12 AS
SELECT 
    sda.product_department_name,
    sda.branch,
    sda.broker_group,
    sda.SUBMISSION_COUNT,
    sda.BOUND_COUNT,
    sda.BROKER_GROUP_BINDING_RATIO,
    COALESCE(md.broker_group_median_premium, 0) AS BROKER_GROUP_MEDIAN_PREMIUM
FROM temp_excess_casualty_metrics sda
LEFT JOIN temp_excess_casualty_median md
    ON sda.product_department_name = md.product_department_name
   AND sda.branch = md.branch
   AND sda.broker_group = md.broker_group
ORDER BY sda.SUBMISSION_COUNT DESC;

-- calculate the decile for broker group binding ratio and median value using qunatiles
ALTER TABLE temp_excess_casualty_rule12
ADD COLUMN BROKER_GROUP_BINDING_RATIO_DECILE INTEGER;

ALTER TABLE temp_excess_casualty_rule12
ADD COLUMN BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER;

WITH ranked_data AS (
    SELECT
        BROKER_GROUP_BINDING_RATIO,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_BINDING_RATIO) AS percentile_bindingratio,
        BROKER_GROUP_MEDIAN_PREMIUM,
        PERCENT_RANK() OVER (ORDER BY BROKER_GROUP_MEDIAN_PREMIUM) AS percentile_medianpremium,
        BROKER_GROUP_MEDIAN_PREMIUM,
        product_department_name,
        branch,
        broker_group
        FROM
        temp_excess_casualty_rule12 where submission_count>=10 or bound_count >=2
),
computed_data AS (
    SELECT
        product_department_name,
        COALESCE(branch,'NULL') as branch,
        broker_group,
        CASE
            WHEN percentile_bindingratio <= 0.1 THEN 1
            WHEN percentile_bindingratio <= 0.2 THEN 2
            WHEN percentile_bindingratio <= 0.3 THEN 3
            WHEN percentile_bindingratio <= 0.4 THEN 4
            WHEN percentile_bindingratio <= 0.5 THEN 5
            WHEN percentile_bindingratio <= 0.6 THEN 6
            WHEN percentile_bindingratio <= 0.7 THEN 7
            WHEN percentile_bindingratio <= 0.8 THEN 8
            WHEN percentile_bindingratio <= 0.9 THEN 9
            ELSE 10
        END AS decile1,
        CASE
            WHEN percentile_medianpremium <= 0.1 THEN 1
            WHEN percentile_medianpremium <= 0.2 THEN 2
            WHEN percentile_medianpremium <= 0.3 THEN 3
            WHEN percentile_medianpremium <= 0.4 THEN 4
            WHEN percentile_medianpremium <= 0.5 THEN 5
            WHEN percentile_medianpremium <= 0.6 THEN 6
            WHEN percentile_medianpremium <= 0.7 THEN 7
            WHEN percentile_medianpremium <= 0.8 THEN 8
            WHEN percentile_medianpremium <= 0.9 THEN 9
            ELSE 10
        END AS decile2
    FROM ranked_data
)
UPDATE temp_excess_casualty_rule12
SET BROKER_GROUP_BINDING_RATIO_DECILE = computed_data.decile1,
BROKER_GROUP_MEDIAN_PREMIUM_DECILE = computed_data.decile2
FROM computed_data
WHERE temp_excess_casualty_rule12.product_department_name = computed_data.product_department_name
AND COALESCE(temp_excess_casualty_rule12.branch,'NULL') = computed_data.branch
AND temp_excess_casualty_rule12.broker_group = computed_data.broker_group;

select * from temp_excesss_casualty_rule12;

---------------------------------------------------------------------------- Completed Table Creations ----------------------------------------------------------------------------

-- Appending all the four rule12 tables into one
CREATE TEMP TABLE temp_priority_score_rule12_data(
    product_department_name varchar(100),
    branch varchar(50),
    broker_group varchar(100),
    SUBMISSION_COUNT integer,
    BOUND_COUNT integer,
    BROKER_GROUP_BINDING_RATIO DECIMAL,
    BROKER_GROUP_MEDIAN_PREMIUM DECIMAL,
    BROKER_GROUP_BINDING_RATIO_DECILE INTEGER,
    BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER
);
insert into temp_priority_score_rule12_data
select * from temp_env_rule12;

insert into temp_priority_score_rule12_data
select * from temp_a_and_e_rule12;

INSERT INTO temp_priority_score_rule12_data
SELECT * FROM temp_pc_rule12;

INSERT INTO temp_priority_score_rule12_data
SELECT * FROM temp_onshore_construction_rule12;

INSERT INTO temp_priority_score_rule12_data
SELECT * FROM temp_excesss_casualty_rule12;

-- create the pripority score for rule3
CREATE TABLE IF NOT EXISTS ps_broker_group_priority_score(
    product_department_name varchar(100),
    branch varchar(50),
    broker_group varchar(100),
    submission_count integer,
    bound_count integer,
    BROKER_GROUP_BINDING_RATIO DECIMAL,
    BROKER_GROUP_MEDIAN_PREMIUM DECIMAL,
    BROKER_GROUP_BINDING_RATIO_DECILE INTEGER,
    BROKER_GROUP_MEDIAN_PREMIUM_DECILE INTEGER);
    
truncate table ps_broker_group_priority_score;
insert into ps_broker_group_priority_score
SELECT * FROM temp_priority_score_rule12_data;

alter table ps_broker_group_priority_score drop column refresh_date;
alter table ps_broker_group_priority_score add column refresh_date date default current_date;

drop table if exists
    temp_env_median,
    temp_env_metrics,
    temp_env_rule12,
    temp_a_and_e_median,
    temp_a_and_e_metrics,
    temp_a_and_e_rule12,
    temp_pc_median,
    temp_pc_metrics,
    temp_pc_rule12,
    temp_onshore_construction_median,
    temp_onshore_construction_metrics,
    temp_onshore_construction_rule12,
    temp_excess_casualty_median,
    temp_excess_casualty_rule12,
    temp_excess_casualty_metrics,
    temp_priority_score_rule12_data;

select count(*) from ps_broker_group_priority_score;

select * from ps_broker_group_priority_score;
