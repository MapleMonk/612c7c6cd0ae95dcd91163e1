{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.ellementry_SWIGGY_SALES_FACT_ITEMS_INTERMEDIATE AS SELECT distinct concat(Store_id,\'-\',fi.item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, DATE(TIMESTAMP(ordered_Date)) AS order_date, upper(fi.city) as CITY, VARIANT, CAST(base_mrp as float64)*CAST(UNITS_SOLD as float64) as MRP, STORE_ID, AREA_NAME, fi.ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, UPPER(cast(fi.L2_category as string)) AS product_category, UPPER(cast(fi.L2_category as string)) AS product_sub_category FROM MapleMonk.Ellementry_Swiggy_sales fi ; CREATE OR REPLACE TABLE MAPLEMONK.ellementry_SWIGGY_SALES_FACT_ITEMS AS SELECT fi.order_id, fi.selling_price, fi.order_date, upper(fi.city) as city, fi.variant, fi.mrp, fi.store_id, fi.area_name, fi.item_code, fi.quantity, UPPER(cast(fi.product_category as string)) AS product_category, UPPER(cast(fi.product_sub_category as string)) AS product_sub_category FROM MapleMonk.ellementry_SWIGGY_SALES_FACT_ITEMS_INTERMEDIATE fi ;",
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
            