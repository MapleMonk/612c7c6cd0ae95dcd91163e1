{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Ritualistic_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,COALESCE( SAFE.PARSE_DATE(\'%d-%b-%Y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%d-%m-%y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(METRICS_DATE)) ) AS Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Product_Name ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend ,\'Manual\' AS Type from MapleMonk.Ritualistic_SWIGGY_ADS ; CREATE OR REPLACE TABLE MAPLEMONK.Ritualistic_SWIGGY_SALES_FACT_ITEMS AS SELECT \'SWIGGY\' AS CHANNEL, \'PRODUCT SALES\' AS AD_TYPE, SAFE_CAST(NULL AS FLOAT64) AS CTR, CAST(COALESCE( SAFE.PARSE_DATE(\'%d-%b-%Y\', TRIM(ORDERED_DATE)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(ORDERED_DATE)), SAFE.PARSE_DATE(\'%d-%m-%y\', TRIM(ORDERED_DATE)), SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(ORDERED_DATE))) as date) AS Date, SAFE_CAST(NULL AS FLOAT64) AS Views, SAFE_CAST(NULL AS FLOAT64) AS Clicks, ITEM_CODE AS Campaign_ID, SAFE_CAST(NULL AS FLOAT64) AS Impressions, PRODUCT_NAME, BRAND AS Campaign_Name, SAFE_CAST(GMV AS FLOAT64) AS ad_sales, SAFE_CAST(SAFE_CAST(BASE_MRP AS FLOAT64) * SAFE_CAST(UNITS_SOLD AS FLOAT64) AS FLOAT64) AS spend, \'Manual\' AS Type FROM MapleMonk.Ritualistic_SWIGGY_SALES;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            