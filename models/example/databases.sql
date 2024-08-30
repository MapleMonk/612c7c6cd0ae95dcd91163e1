{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table andamen_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from andamen_db.MAPLEMONK.Shopify_andamen_website_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from andamen_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            