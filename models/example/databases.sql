{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.offline_master_Daily_Report AS WITH DateCheck AS ( SELECT 1 AS Exist FROM snitch_db.maplemonk.offline_master_Daily_Report WHERE DATE = CURRENT_DATE() LIMIT 1 ) SELECT *, current_date FROM snitch_db.maplemonk.offline_master WHEre not EXISTS (SELECT * FROM DateCheck) GROUP BY 1, 2, 3, 4 union all select * FROM snitch_db.maplemonk.offline_master_Daily_Report;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            