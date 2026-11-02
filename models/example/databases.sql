{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table RUBYSORGANICS_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from RUBYSORGANICS_DB.MAPLEMONK.Shopify_rubysorganics_UTM_Parameters ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RUBYSORGANICS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            