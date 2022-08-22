{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from someStuff cost source",
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
                        