{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.transport_costs_last_mid_mile ADD (ddate \'Date\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.transport_costs_last mid mile
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        