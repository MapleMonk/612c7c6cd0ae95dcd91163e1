{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET LogDate = TRY_TO_DATE(\"date(Date)\",\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.EPM_Sheet1
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        