{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_New_MP_Primary_Sales_Table` AS WITH PandL AS ( SELECT Date, MARKETPLACE, COLLECTION, PRODUCT_CATEGORY, FINAL_MARKETPLACE, Date_pre, channel, SOURCE, REFERENCE_CODE, SKU, SALEORDERITEMCODE, COMMONSKU, PRODUCT_SUB_CATEGORY, PRODUCT_TYPE, PRINT, category_code, SALES, return_sales, quantity, Marketplace_Segment FROM `MapleMonk.zouk_pandl` where not(lower(final_Marketplace) like any (\'%shopify%\',\'%website%\',\'app\',\'%offline%\',\'ebo\')) ) SELECT * FROM PandL;",
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
            