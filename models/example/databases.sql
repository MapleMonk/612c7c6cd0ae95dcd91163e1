{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table lagorii_db.maplemonk.funnel_metrics_by_date as select to_date(a.date,\'yyyymmdd\') date, sessions, checkouts, addtocarts, sales.orders from lagorii_db.maplemonk.ga4_lagorii_funnelbydate a left join (select order_date::Date date, count(distinct reference_code) orders from lagorii_db.maplemonk.lagorii_db_sales_consolidated where marketplace = \'SHOPIFY_LAGORII_KIDS\' group by 1 ) sales on to_date(a.date,\'yyyymmdd\') = sales.date",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from lagorii_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            