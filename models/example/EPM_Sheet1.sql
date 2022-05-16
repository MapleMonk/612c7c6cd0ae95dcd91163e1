{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.epm_sheet1 ADD (date Date); UPDATE eggozdb.maplemonk.zero_bill_one_month SET date = TRY_TO_DATE(\"date(date)\",\'DD/MM/YYYY\');",
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
                        