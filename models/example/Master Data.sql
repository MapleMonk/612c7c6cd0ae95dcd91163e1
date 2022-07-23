{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.master_data ADD (GRN_date Date); UPDATE eggozdb.maplemonk.master_data SET GRN_date = DATE_TRUNC(day,TRY_CONVERT(DATETIME,\"GRN_Date\"));",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.Master Data
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        