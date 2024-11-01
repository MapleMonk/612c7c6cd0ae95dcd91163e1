{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ..Shopify_UTM_Parameters_fact_item as select * from ..Shopify_mydesignation_store_UTM_Parameters ;",
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
            