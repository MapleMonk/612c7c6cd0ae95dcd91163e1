{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table RPSG_DB.MAPLEMONK.UTM_DATA as select order_date,reference_code,final_source,final_medium ,final_utm_campaign,ga_campaign,shopify_utm_campaign from RPSG_DB.MAPLEMONK.SALES_CONSOLIDATED_DRV WHERE marketplace IN (\'SHOPIFY_DRV\', \'SHOPIFY_AYURVEDICSOURCE\', \'SHOPIFY_HERBOBUILD\', \'Woocommerce\', \'Woocommerce 2\', \'Woocommerce 3\')",
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
            