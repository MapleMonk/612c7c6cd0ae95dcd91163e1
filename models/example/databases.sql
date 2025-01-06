{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table gharsoaps_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from gharsoaps_db.MAPLEMONK.Shopify_ghar_soaps_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from gharsoaps_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            