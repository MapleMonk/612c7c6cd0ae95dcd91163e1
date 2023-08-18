{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.BI_last_mid_mile ADD (ddate \'Date\'); UPDATE eggozdb.maplemonk.BI_last_mid_mile SET ddate = TRY_TO_DATE(\'Date\',\'DD/MM/YYYY\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        