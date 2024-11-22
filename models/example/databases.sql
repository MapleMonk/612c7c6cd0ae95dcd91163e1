{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.Zouk_Product_Sales_Cost_Source as select Date, MARKETPLACE, COLLECTION, PRODUCT_CATEGORY, FINAL_MARKETPLACE, channel, SOURCE, TRADE_MARGIN, REFERENCE_CODE, SKU, COMMONSKU, PRODUCT_SUB_CATEGORY, PRINT, category_code, BAU_MRP_SALES, BAU_DISCOUNT, SALES, Spend, Brand_Spend from maplemonk.zouk_pandl",
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
            