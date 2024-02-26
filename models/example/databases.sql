{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from JPEARLS_DB.MAPLEMONK.JPEARLS_RETAIL_ORDERS_DATA limit 10",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from JPEARLS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        