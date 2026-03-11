{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.asaya_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,COALESCE( SAFE.PARSE_DATE(\'%d-%b-%Y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%d-%m-%y\', TRIM(METRICS_DATE)), SAFE.PARSE_DATE(\'%m/%d/%Y\', TRIM(METRICS_DATE))) AS Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Product_Name ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend ,\'Manual\' AS Type from MapleMonk.Asaya_Swiggy_ads ; CREATE OR REPLACE TABLE MAPLEMONK.asaya_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, CAST(left(ordered_Date,10) as date) as order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, UPPER(cast(L2_category as string)) AS product_category, UPPER(cast(L2_category as string)) AS product_sub_category FROM Maplemonk.Asaya_Swiggy_sales;",
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
            