{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Carlton_London_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, CAST(ordered_Date as date) as order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE as product_id, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(null as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(null as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, null as commonsku, upper(fi.PRODUCT_NAME) as PRODUCT_NAME FROM MapleMonk.Instamart_Carlton_London_SALES fi ;",
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
            