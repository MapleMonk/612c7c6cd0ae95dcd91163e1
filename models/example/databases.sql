{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "",
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
                        