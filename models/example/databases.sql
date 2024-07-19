{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.availability_master_snapshot_updated as select * from snitch_db.maplemonk.availability_master_snapshot_updated union all select *,current_date() as date from snitch_db.maplemonk.availability_master;",
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
                        