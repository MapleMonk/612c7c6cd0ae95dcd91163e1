{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table Sunbeam_DB.Maplemonk.Shopify_UTM_Parameters_fact_item as select * from Sunbeam_DB.Maplemonk.Shopify_sunbeam_ting_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from Sunbeam_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            