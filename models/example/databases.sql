{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.Order_Over_Issues_v3 AS SELECT orders.ORDER_DATE, orders.daily_order_count, COALESCE(ticket_counts.daily_ticket_count, 0) AS daily_ticket_count, COALESCE((COALESCE(ticket_counts.daily_ticket_count, 0) * 100.0) / NULLIF(orders.daily_order_count, 0), 0) AS ticket_percentage FROM ( SELECT ORDER_DATE, COUNT(DISTINCT ORDER_NAME) AS daily_order_count FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE ORDER_DATE >= \'2024-07-01\' AND ORDER_DATE <= CURRENT_DATE AND MARKETPLACE_MAPPED = \'SHOPIFY\' GROUP BY ORDER_DATE ) AS orders LEFT JOIN ( SELECT CAST( \"Created date\" as date) AS Converted_Created_At, count(DISTINCT \"External ticket Id\") AS daily_ticket_count FROM maplemonk.freshchat_bot_conversations WHERE CAST( \"Created date\" as date) >= \'2024-07-01\' and status =\'Closed\' and flow <> \'Resolve - Campaigns flow\' GROUP BY CAST( \"Created date\" as date) ) AS ticket_counts ON orders.ORDER_DATE = ticket_counts.Converted_Created_At ORDER BY orders.ORDER_DATE DESC;",
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
            