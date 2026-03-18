{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.carlington_Blinkit_Fact_items AS SELECT Order_ID as order_id, COALESCE( DATE(SAFE.PARSE_TIMESTAMP(\'%Y-%m-%dT%H:%M:%S\', Order_Date)), SAFE.PARSE_DATE(\'%e %b %Y\', Order_Date)) AS order_date, cast(MRP__Rs_ as float64) mrp, Customer_City as city, Item_ID as product_id, cast(replace(Quantity,\'.0\',\'\') as int64) quantity, Product_Name as product_name, bs.L0_Category as product_category, L1_Category as sub_category, L2_Category as sub_sub_category, Product_name as Product_name_final, cast(Selling_Price__Rs_ as float64) as Selling_Price_final FROM `maplemonk.carlington_blinkit_sales` bs ;",
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
            