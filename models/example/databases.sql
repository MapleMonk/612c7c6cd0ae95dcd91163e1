{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.snitchit_analytics_web_table as ( select to_date(date,\'YYYYMMDD\') as date, operatingsystem, itempromotionname, itempromotionclickthroughrate as ctr, itemsclickedinpromotion as clicks, itemsviewedinpromotion as views from snitch_db.maplemonk.snitchit_analytics_web_snitchit_analytics_web );",
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
            