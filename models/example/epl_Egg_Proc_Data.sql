{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.Epl_egg_proc_data ADD (GRN_Date date); UPDATE eggozdb.maplemonk.Epl_egg_proc_data SET GRN_Date = TRY_TO_DATE(\"GRN Date\",\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.epl_Egg_Proc_Data
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        