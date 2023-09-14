{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table dunatura_db.maplemonk.Shopify_UTM_Parameters_fact_item as select * from dunatura_db.maplemonk.Shopify_dunatura_de_Shopify_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from dunatura_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        