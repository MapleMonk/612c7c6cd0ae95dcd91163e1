{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table opensecret_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from opensecret_db.MAPLEMONK.SHOPIFY_ORDER_ATTRIBUTION_OPEN_SECRET_STORE_UTM_PARAMETERS;",
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
                        