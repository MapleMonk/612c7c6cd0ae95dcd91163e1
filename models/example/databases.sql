{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.Global_pareto_v2_Daily_Report_1 AS WITH DateCheck AS ( SELECT 1 AS Exist FROM snitch_db.maplemonk.Global_pareto_v2_Daily_Report_1 WHERE DATE = CURRENT_DATE() LIMIT 1 ) SELECT *, current_date as date FROM snitch_db.maplemonk.Global_pareto_v2 WHEre not EXISTS (SELECT * FROM DateCheck) union all select * FROM snitch_db.maplemonk.Global_pareto_v2_Daily_Report_1;",
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
            