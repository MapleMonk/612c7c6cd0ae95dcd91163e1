{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table offduty_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from offduty_db.MAPLEMONK.Shopify_offdutystore_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from offduty_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        