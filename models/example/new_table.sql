{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "yrdhtgfkflgg;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from TEST.TEST.new_table
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        