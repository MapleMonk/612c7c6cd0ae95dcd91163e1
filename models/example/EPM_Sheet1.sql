{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET LogDate = TRY_TO_DATE(\"DATE\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_sheet1 SET Pdate = TRY_TO_DATE(\"Processing Date\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_pkg_loss ADD (LogDate Date); UPDATE eggozdb.maplemonk.epm_pkg_loss SET LogDate = TRY_TO_DATE(\"DATE\",\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.epm_pkg_loss ADD (Pdate Date); UPDATE eggozdb.maplemonk.epm_pkg_loss SET Pdate = TRY_TO_DATE(\"Packed Date\",\'DD/MM/YYYY\');",
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
                        