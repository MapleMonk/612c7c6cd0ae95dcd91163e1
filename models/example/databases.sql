{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Zouk_Product_Secondary_Sales_Cost_Source AS SELECT zp.Date, zp.MARKETPLACE, zp.COLLECTION, zp.PRODUCT_CATEGORY, zp.FINAL_MARKETPLACE, nm.channel, zp.SOURCE, zp.TRADE_MARGIN, zp.REFERENCE_CODE, zp.SKU, zp.COMMONSKU, zp.PRODUCT_SUB_CATEGORY, zp.PRINT, zp.category_code, zp.BAU_MRP_SALES, zp.BAU_DISCOUNT, zp.SALES, zp.Spend, zp.Brand_Spend, zp.quantity FROM maplemonk.zouk_secondary_pandl zp LEFT JOIN maplemonk.zouk_New_Marketplace_Mapping nm ON LOWER(zp.MARKETPLACE) = LOWER(nm.marketplace);",
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
            