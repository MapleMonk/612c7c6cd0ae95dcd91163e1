{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from somestuff;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MM_TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        