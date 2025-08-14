{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `Maplemonk.Habanero_Swiggy_Fact_Items` AS SELECT SAFE_CAST(GMV AS FLOAT64) AS GMV, CAST(CITY AS STRING) AS CITY, CAST(BRAND AS STRING) AS BRAND, CAST(COMBO AS STRING) AS COMBO, SAFE_CAST(SAFE_CAST(Index AS FLOAT64) AS INT64) AS Index, CAST(VARIANT AS STRING) AS VARIANT, SAFE_CAST(BASE_MRP AS FLOAT64) AS BASE_MRP, CAST(STORE_ID AS STRING) AS STORE_ID, CAST(AREA_NAME AS STRING) AS AREA_NAME, CAST(ITEM_CODE AS STRING) AS ITEM_CODE, SAFE_CAST(SAFE_CAST(UNITS_SOLD AS FLOAT64) AS INT64) AS UNITS_SOLD, CAST(L1_CATEGORY AS STRING) AS L1_CATEGORY, CAST(L2_CATEGORY AS STRING) AS L2_CATEGORY, CAST(L3_CATEGORY AS STRING) AS L3_CATEGORY, SAFE_CAST(ORDERED_DATE AS TIMESTAMP) AS ORDERED_DATE, CAST(PRODUCT_NAME AS STRING) AS PRODUCT_NAME, CAST(COMBO_ITEM_CODE AS STRING) AS COMBO_ITEM_CODE, SAFE_CAST(SAFE_CAST(COMBO_UNITS_SOLD AS FLOAT64) AS INT64) AS COMBO_UNITS_SOLD FROM `Maplemonk.Habanero_Swiggy_db_sales`;",
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
            