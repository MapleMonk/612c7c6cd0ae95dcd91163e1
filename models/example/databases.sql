{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table RPSG_DB.MAPLEMONK.UTM_DATA as SELECT scd.ORDER_DATE, scd.FINAL_CHANNEL, scd.REFERENCE_CODE, scd.FINAL_SOURCE, scd.FINAL_MEDIUM, scd.FINAL_UTM_CAMPAIGN, scd.GA_CAMPAIGN, scd.SHOPIFY_UTM_CAMPAIGN, fisd.GOKWIK_UTM_CONTENT FROM RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_DRV scd LEFT JOIN RPSG_DB.MAPLEMONK.FACT_ITEMS_SHOPIFY_DRV fisd ON scd.REFERENCE_CODE = fisd.ORDER_NAME WHERE scd.MARKETPLACE IN (\'SHOPIFY_DRV\', \'SHOPIFY_AYURVEDICSOURCE\', \'SHOPIFY_HERBOBUILD\', \'Woocommerce\', \'Woocommerce 2\', \'Woocommerce 3\') and order_date >= \'2025-02-01\'",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            