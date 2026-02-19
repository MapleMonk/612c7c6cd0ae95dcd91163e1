{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE andamen_db.maplemonk.websitefunnel_avg_basket_size AS WITH session_agg AS ( SELECT date, UPPER(channel) AS channel, SUM(sessions) AS sessions FROM andamen_db.MAPLEMONK.andamen_db_ga_sessions_consolidated GROUP BY 1,2 ), orders AS ( SELECT DISTINCT s.order_id, s.sku, s.order_status, S.FINAL_SHIPPING_STATUS, s.order_date, s.marketplace, s.channel, s.overall_customer_flag, sum(selling_price) as selling_price FROM andamen_db.MAPLEMONK.andamen_db_sales_consolidated s WHERE LOWER(s.overall_customer_flag) IN (\'new\',\'repeat\') and marketplace = \'SHOPIFY\' group by 1,2,3,4,5,6,7,8 ) SELECT s.selling_price, s.sku, s.order_id, coalesce(s.order_date, sa.date) as order_date, coalesce(s.channel, sa.channel) as channel, s.overall_customer_flag, s.order_status, s.final_shipping_status, div0(sa.sessions, count(1) over (partition by coalesce(s.order_date, sa.date), coalesce(s.channel, sa.channel))) as sessions FROM orders s full outer JOIN session_agg sa ON s.order_date = sa.date AND UPPER(s.channel) = sa.channel",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from andamen_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            