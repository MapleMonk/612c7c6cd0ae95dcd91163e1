{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MAPLEMONK_DEMO_DB.MAPLEMONK_CUSTOMER.Shopify_UTM_Parameters_fact_item as select * from MAPLEMONK_DEMO_DB.MAPLEMONK_CUSTOMER.shopify_perfora_care_Shopify_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MAPLEMONK_DEMO_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        