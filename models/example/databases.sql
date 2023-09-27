{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table BSC_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from BSC_DB.MAPLEMONK.Shopify_bombay_shaving_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        