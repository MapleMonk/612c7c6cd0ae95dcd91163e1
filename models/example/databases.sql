{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MYMUSE_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from MYMUSE_DB.MAPLEMONK.Shopify_muse_india_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MYMUSE_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        