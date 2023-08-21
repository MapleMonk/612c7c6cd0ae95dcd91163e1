{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.Maplemonk.Shopify_UTM_Parameters_fact_item as select * from skinq_db.Maplemonk.Shopify_skin_q_UTM_Parameters ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from skinq_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        