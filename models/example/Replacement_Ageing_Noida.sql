{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.replacement_ageing_noida ADD (Ddate Date); UPDATE eggozdb.maplemonk.replacement_ageing_noida SET Ddate = TRY_TO_DATE(\"Receiving Date\");",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.Replacement_Ageing_Noida
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        