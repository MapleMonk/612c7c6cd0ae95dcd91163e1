{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MAPLEMONKTEST185_DB.Maplemonk.Shopify_UTM_Parameters_fact_item as select * from MAPLEMONKTEST185_DB.Maplemonk.Shopify_gladful_Shopify_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MAPLEMONKTEST185_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        