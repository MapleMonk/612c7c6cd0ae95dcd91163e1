{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.Zouk_Product_Sales_Cost_Source as select Date, MARKETPLACE, COLLECTION, PRODUCT_CATEGORY, FINAL_MARKETPLACE, channel, new_customer_flag, customer_id_final, SOURCE, TRADE_MARGIN, REFERENCE_CODE, SKU, COMMONSKU, PRODUCT_TYPE, PRINT, category_code, BAU_MRP_SALES, BAU_DISCOUNT, SALES, Spend, Brand_Spend, return_mrp_sales, return_trade_margin, Returns, gst, Return_GST, quantity from maplemonk.zouk_pandl",
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
            