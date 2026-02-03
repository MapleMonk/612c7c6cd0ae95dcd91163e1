{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.snitchit_analytics_table as ( select to_date(date,\'YYYYMMDD\') as date, operatingsystem, itempromotionname, \'app\' as type, itempromotionclickthroughrate as ctr, itemsclickedinpromotion as clicks, itemsviewedinpromotion as views from snitch_db.maplemonk.snitchit_analytics union all select to_date(date,\'YYYYMMDD\') as date, operatingsystem, itempromotionname, \'web\' as type, itempromotionclickthroughrate as ctr, itemsclickedinpromotion as clicks, itemsviewedinpromotion as views from snitch_db.maplemonk.snitchit_analytics_web_snitchit_analytics_web union all select to_date(date,\'YYYYMMDD\') as date, operatingsystem, itempromotionname, \'plus\' as type, itempromotionclickthroughrate as ctr, itemsclickedinpromotion as clicks, itemsviewedinpromotion as views from snitch_db.maplemonk.snitchit_analytics_plus )",
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
            