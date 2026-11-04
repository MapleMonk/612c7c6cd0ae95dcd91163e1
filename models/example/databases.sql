{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table SUNBEAM_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from SUNBEAM_DB.MAPLEMONK.Shopify_sunbeam_ting_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SUNBEAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            