{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE or replace TABLE snitch_db.maplemonk.AVAILABILITY_MASTER_V2_Daily_Report AS SELECT *, CURRENT_DATE AS date FROM snitch_db.maplemonk.AVAILABILITY_MASTER_V2 WHERE 1=0; ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; INSERT INTO snitch_db.maplemonk.AVAILABILITY_MASTER_V2_Daily_Report SELECT * FROM ( SELECT *, CURRENT_DATE AS date FROM snitch_db.maplemonk.AVAILABILITY_MASTER_V2 ) AS source_data WHERE NOT EXISTS ( SELECT 1 FROM snitch_db.maplemonk.AVAILABILITY_MASTER_V2_Daily_Report AS target_data WHERE source_data.date = target_data.date );",
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
            