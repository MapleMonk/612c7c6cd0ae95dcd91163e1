{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from TEST.MONGO.tables;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        