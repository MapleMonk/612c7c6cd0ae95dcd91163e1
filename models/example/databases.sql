{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.BI_last_mile_maplemonk ADD (ddate Date); UPDATE eggozdb.maplemonk.BI_last_mile_maplemonk SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.BI_first_mile_maplemonk ADD (ddate Date); UPDATE eggozdb.maplemonk.BI_first_mile_maplemonk SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\');",
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
                        