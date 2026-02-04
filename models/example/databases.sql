{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.misschase_zepto_fact_items_intermediate as select EAN, concat(EAN,sku_number,city,date) as order_item, CAST(MRP AS FLOAT64) AS MRP, CITY, CAST(Date AS timestamp) as Order_Date, SKU_NAME AS Product_name, Brand_Name as Brand, sku_number, sku_category, cast(Gross_Merchandise_Value as float64) as selling_price, manufacturer_id, SKU_Sub_Category, cast(Gross_Merchandise_Value as float64) as Gross_Selling_Value, cast(Sales__Qty____Units as int64) as quantity, cast(Gross_Merchandise_Value as float64) as Gross_Merchandise_Value from `MAPLEMONK.Zepto_miss_chase_sales` ; create or replace table maplemonk.misschase_zepto_fact_items as select z.* ,CONCAT(EAN,\'-\',ORDER_DATE,\'-\',CITY,\'-\',PRODUCT_NAME,\'-\',SKU_NUMBER,\'-\',QUANTITY,\'-\',GROSS_SELLING_VALUE) AS ORDER_ID ,replace(replace(UPPER(SPLIT(REPLACE(LOWER(z.Product_name), \'miss chase \', \'\'), \'|\')[SAFE_OFFSET(0)]),\'1.0 PIECE\',\'\'),\'WOMEN S\',\"\"\"WOMEN\'S\"\"\") AS product_name_final ,COALESCE(UPPER(cast(z.sku_category as string))) AS product_category ,COALESCE(upper(z.SKU_Sub_Category)) AS sub_category from maplemonk.misschase_zepto_fact_items_intermediate z ;",
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
            