{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ras_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from ras_db.MAPLEMONK.Shopify_ras_luxury_oils_india_UTM_Parameters UNION select * from ras_db.MAPLEMONK.Shopify_ras_in_store_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ras_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        