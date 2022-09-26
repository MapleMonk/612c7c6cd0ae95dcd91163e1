{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from metadata;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from STRANGE959_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        