{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Atovio_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, CAST(left(ordered_Date,10) as date) as order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, UPPER(cast(L2_category as string)) AS product_category, UPPER(cast(L2_category as string)) AS product_sub_category, UPPER(PRODUCT_NAME) as Product_Name FROM maplemonk.sales_report_sales;",
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
            