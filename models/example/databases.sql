{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table upurfit_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from upurfit_db.MAPLEMONK.Shopify_upurfit_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from upurfit_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            