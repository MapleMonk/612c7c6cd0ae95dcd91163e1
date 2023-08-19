{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.BI_LM ADD (ddate Date); UPDATE eggozdb.maplemonk.BI_LM SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\'); ALTER TABLE eggozdb.maplemonk.BI_FM ADD (ddate Date); UPDATE eggozdb.maplemonk.BI_FM SET ddate = TRY_TO_DATE(Date,\'DD/MM/YYYY\');",
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
                        