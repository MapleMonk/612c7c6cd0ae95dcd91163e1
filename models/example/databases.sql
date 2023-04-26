{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table opensecret_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from opensecret_db.MAPLEMONK.Shopify_Order_Attribution_open_secret_store_Shopify_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from opensecret_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        