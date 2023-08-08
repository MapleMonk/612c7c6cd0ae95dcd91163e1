{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table indusvalley_db.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from indusvalley_db.MAPLEMONK.Shopify_good_roots_kitchenware_private_limited_Shopify_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from indusvalley_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        