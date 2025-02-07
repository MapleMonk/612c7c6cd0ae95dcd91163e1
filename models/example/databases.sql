{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table sleepycat_db.maplemonk.Test_Retail_walkins_fact_items as select date::date as Date, walkins::int as walkins, store_id, conversions::int conversions from sleepycat_db.maplemonk.sleepycat_db_retail_data_test",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SLEEPYCAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            