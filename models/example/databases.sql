{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table FUAARK_DB.MAPLEMONK.Shopify_UTM_Parameters_fact_item as select * from FUAARK_DB.MAPLEMONK.Shopify_fuaarkindia_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from FUAARK_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        