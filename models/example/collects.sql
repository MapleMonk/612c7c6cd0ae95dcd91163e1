{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from MAPLEMONK_DEV.MAPLEMONK.COLLECTS;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MAPLEMONK_DEV.MAPLEMONK.collects
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        