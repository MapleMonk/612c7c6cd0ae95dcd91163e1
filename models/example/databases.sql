{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.zouk_sales_cost_source_DTC as select * from maplemonk.zouk_sales_cost_source where lower(Marketplace) like any (\'%shopify%\',\'%website%\',\'app\') ;",
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
            