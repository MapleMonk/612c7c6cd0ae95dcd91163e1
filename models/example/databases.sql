{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table Boxseat_ventures_private_Limited931_DB.Maplemonk.Shopify_UTM_Parameters_fact_item as select * from Boxseat_ventures_private_Limited931_DB.Maplemonk.Shopify_10clubhomes_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Boxseat_ventures_private_Limited931_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        