{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.Zouk_Product_Sales_Cost_Source as select Date, zp.MARKETPLACE, COLLECTION, PRODUCT_CATEGORY, zp.FINAL_MARKETPLACE, zp.Marketplace_Segment, UPPER(nm.Channel) AS Channel, new_customer_flag, customer_id_final, SOURCE, TRADE_MARGIN, REFERENCE_CODE, SKU, COMMONSKU, PRODUCT_TYPE, PRINT, category_code, BAU_MRP_SALES, BAU_DISCOUNT, SALES, Spend, Brand_Spend, return_mrp_sales, return_trade_margin, Returns, gst, Return_GST, quantity, Channel_Margin, Cogs, return_cogs, offline_store_cost, logistics_cost, return_logistics_cost, other_performance_spend from maplemonk.zouk_pandl zp LEFT JOIN ( SELECT * FROM maplemonk.Zouk_db_New_Marketplace_Mapping QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(marketplace)) ORDER BY 1) = 1 ) nm ON LOWER(TRIM(zp.Marketplace)) = LOWER(TRIM(nm.Marketplace)) ;",
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
            