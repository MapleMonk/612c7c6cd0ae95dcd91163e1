{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select * from someStuff cost source",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from STV_TBL_PERM
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        