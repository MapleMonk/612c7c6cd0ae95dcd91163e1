{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table andamen_db.maplemonk.websitefunnel_avg_basket_size as SELECT s.selling_price, s.sku, s.order_id, s.order_date, s.marketplace, s.channel, s.overall_customer_flag, s.order_status, s.final_shipping_status, COUNT(DISTINCT s.order_id) AS orders, SUM(COALESCE(sc.sessions, 0)) AS sessions, CASE WHEN SUM(COALESCE(sc.sessions, 0)) = 0 THEN 0 ELSE ROUND((COUNT(DISTINCT s.order_id) / SUM(COALESCE(sc.sessions, 0))) * 100, 2) END AS conversion_percentage, SUM(s.selling_price) AS total_revenue FROM andamen_db.MAPLEMONK.andamen_db_sales_consolidated s LEFT JOIN andamen_db.MAPLEMONK.andamen_db_ga_sessions_consolidated sc ON s.order_date = sc.date AND UPPER(s.channel) = UPPER(sc.channel) AND UPPER(s.marketplace) = UPPER(sc.shop_name) WHERE LOWER(s.overall_customer_flag) IN (\'new\', \'repeat\') GROUP BY s.selling_price, s.sku, s.order_id, s.order_date, s.marketplace, s.channel, s.overall_customer_flag, s.order_status, s.final_shipping_status ORDER BY s.order_date DESC, s.marketplace, s.channel, s.overall_customer_flag, s.order_status, s.final_shipping_status;",
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
            