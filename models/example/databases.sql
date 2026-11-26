{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table JLT_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from JLT_DB.MAPLEMONK.Shopify_justlilthings_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from JLT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            