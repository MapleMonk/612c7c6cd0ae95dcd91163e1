{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Hyuman_Blinkit_Fact_items AS SELECT Order_ID as order_id, COALESCE(DATE(PARSE_TIMESTAMP(\'%Y-%m-%dT%H:%M:%S\', Order_Date)),PARSE_DATE(\'%e %b %Y\', Order_Date)) AS order_date, order_status, cast(MRP__Rs_ as float64) mrp, upper(Customer_City) as city, upper(Customer_state) as state, Item_ID as product_id, upper(business_category) as category, cast(replace(Quantity,\'.0\',\'\') as int64) quantity, Product_Name as product_name, cast(MRP__Rs_ as float64) as MRP_Sales, cast(Total_tax as float64) as Tax, cast(Selling_Price__Rs_ as float64) as Selling_Price, cast(Total_Gross_Bill_Amount as float64) as Gross_Amount, FROM `maplemonk.hyuman_blinkit_seller_sales` bs ;",
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
            