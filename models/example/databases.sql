{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.neshanka_zepto_fact_items_intermediate as select EAN, concat(EAN,sku_number,city,date,selling_price) as order_item, CAST(MRP AS FLOAT64) AS MRP, CITY, CAST(Date AS timestamp) as Order_Date, SKU_NAME AS Product_name, Brand_Name as Brand, sku_number, sku_category, cast(selling_price as float64) as selling_price, manufacturer_id, SKU_Sub_Category as sub_category, cast(Gross_Selling_Value as float64) as Gross_Selling_Value, cast(Sales__Qty____Units as int64) as quantity, cast(Gross_Merchandise_Value as float64) as Gross_Merchandise_Value from `MAPLEMONK.Zepto_neshanka_sales` ; create or replace table maplemonk.neshanka_zepto_fact_items as select z.* from maplemonk.neshanka_zepto_fact_items_intermediate z",
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
            