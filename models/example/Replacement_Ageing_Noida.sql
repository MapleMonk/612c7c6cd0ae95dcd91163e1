{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.replacement_ageing_noida ADD (Receiving_date Date); UPDATE eggozdb.maplemonk.replacement_ageing_noida SET Receiving_date = TRY_TO_DATE(\"Receiving Date\"); ALTER TABLE eggozdb.maplemonk.replacement_ageing_noida ADD (Stock_date Date); UPDATE eggozdb.maplemonk.replacement_ageing_noida SET Stock_date = TRY_TO_DATE(\"Stock date/Packet date\"); ALTER TABLE eggozdb.maplemonk.replacement_ageing_delhi ADD (Ddate Date); UPDATE eggozdb.maplemonk.replacement_ageing_delhi SET Ddate = TRY_TO_DATE(\"Receiving Date\"); ALTER TABLE eggozdb.maplemonk.replacement_ageing_delhi ADD (Stock_date Date); UPDATE eggozdb.maplemonk.replacement_ageing_delhi SET Stock_date = TRY_TO_DATE(\"Stock date/Packet date\"); ALTER TABLE eggozdb.maplemonk.replacement_ageing_gurgaon ADD (Ddate Date); UPDATE eggozdb.maplemonk.replacement_ageing_gurgaon SET Ddate = TRY_TO_DATE(\"Receiving Date\"); ALTER TABLE eggozdb.maplemonk.replacement_ageing_delhi ADD (Stock_date Date); UPDATE eggozdb.maplemonk.replacement_ageing_delhi SET Stock_date = TRY_TO_DATE(\"Stock date/Packet date\");",
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
                        