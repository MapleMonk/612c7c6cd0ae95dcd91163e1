{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table medmongers_db.maplemonk.medmongers_amazon_traffic_report as select a.parentasin, s.brand, \'AMAZON\' as marketplace, \'AMAZON\' as marketing_channel, to_date(dataendtime) as date, SUM(IFNULL(trafficbyasin:sessions::INT, 0)) AS sessions from medmongers_db.maplemonk.amazon_br_medmongers_get_sales_and_traffic_report_asin a left join (select product_id, brand, from medmongers_db.maplemonk.MEDMONGERS_DB_AMAZON_FACT_ITEMS qualify row_number() over (partition by upper(product_id) order by 1)=1 ) s on lower(s.product_id) = lower(a.parentasin) group by 1,2,3,4,5 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            