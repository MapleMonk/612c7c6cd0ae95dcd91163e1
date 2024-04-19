{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table abros_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from abros_db.MAPLEMONK.Shopify_abros_shoes_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from abros_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        