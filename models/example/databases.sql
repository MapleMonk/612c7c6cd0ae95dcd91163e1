{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ttk_db.Maplemonk.Shopify_UTM_Parameters_fact_item as select * from ttk_db.Maplemonk.Shopify_love_depot_india_UTM_Parameters UNION select * from ttk_db.Maplemonk.Shopify_mschief_in_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ttk_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        