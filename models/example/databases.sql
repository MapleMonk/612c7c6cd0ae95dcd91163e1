{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.KAL_SWIGGY_ADS_FACT_ITEMS AS select \'SWIGGY\' CHANNEL ,\'PRODUCT ADS\' AD_TYPE ,SAFE_CAST(TOTAL_CTR as FLOAT64) as CTR ,PARSE_DATE(\'%Y-%m-%d\', METRICS_DATE) AS Date ,SAFE_CAST(0 as FLOAT64) as Views ,SAFE_CAST(TOTAL_CLICKS as FLOAT64) as Clicks ,Campaign_ID ,SAFE_CAST(TOTAL_IMPRESSIONS as FLOAT64) as Impressions ,Campaign_Name ,SAFE_CAST(TOTAL_ROI as FLOAT64) * SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as ad_sales ,SAFE_CAST(TOTAL_BUDGET_BURNT as FLOAT64) as spend from `MapleMonk.KAL_DB_ads`; CREATE OR REPLACE TABLE MAPLEMONK.KAL_SWIGGY_Sales_FACT_ITEMS AS SELECT SAFE_CAST(_airbyte_unique_key AS STRING) AS unique_key, SAFE_CAST(GMV AS FLOAT64) AS gmv, SAFE_CAST(CITY AS STRING) AS city, SAFE_CAST(BRAND AS STRING) AS brand, SAFE_CAST(COMBO AS STRING) AS combo, SAFE_CAST(Index AS INT64) AS index_col, SAFE_CAST(VARIANT AS STRING) AS variant, SAFE_CAST(BASE_MRP AS FLOAT64) AS base_mrp, SAFE_CAST(STORE_ID AS STRING) AS store_id, SAFE_CAST(AREA_NAME AS STRING) AS area_name, SAFE_CAST(ITEM_CODE AS STRING) AS item_code, SAFE_CAST(UNITS_SOLD AS FLOAT64) AS units_sold, SAFE_CAST(L1_CATEGORY AS STRING) AS l1_category, SAFE_CAST(L2_CATEGORY AS STRING) AS l2_category, SAFE_CAST(L3_CATEGORY AS STRING) AS l3_category, PARSE_DATE(\'%Y-%m-%d\', ORDERED_DATE) AS ordered_date, SAFE_CAST(PRODUCT_NAME AS STRING) AS product_name, SAFE_CAST(COMBO_ITEM_CODE AS STRING) AS combo_item_code, SAFE_CAST(COMBO_UNITS_SOLD AS FLOAT64) AS combo_units_sold FROM `MapleMonk.KAL_DB_sales`;",
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
            