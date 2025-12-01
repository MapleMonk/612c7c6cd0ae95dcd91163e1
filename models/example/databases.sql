{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.actual_aov as ( WITH base AS ( SELECT \'+91\' || RIGHT(REGEXP_REPLACE(phone, \'[^0-9]\', \'\'), 10) AS phone_no, order_name, order_timestamp, sum(quantity) as quantity, sum(gross_sales) as order_amount FROM snitch_db.maplemonk.fact_items_snitch group by 1,2,3 ), with_prev AS ( SELECT base.*, LAG(order_timestamp) OVER (PARTITION BY phone_no ORDER BY order_timestamp) AS prev_order_ts FROM base ), marked AS ( SELECT *, CASE WHEN prev_order_ts IS NULL THEN 1 WHEN DATEDIFF(\'minute\', prev_order_ts, order_timestamp) > 5 THEN 1 ELSE 0 END AS is_new_group FROM with_prev ), grouped AS ( SELECT *, SUM(is_new_group) OVER (PARTITION BY phone_no ORDER BY order_timestamp ROWS UNBOUNDED PRECEDING) AS group_seq FROM marked ), sessionized AS ( SELECT CONCAT(phone_no, \'_\', group_seq) AS combined_order_id, phone_no, MIN(order_timestamp) AS combined_order_timestamp, COUNT(DISTINCT order_name) AS num_original_orders, SUM(quantity) AS total_quantity, SUM(order_amount) AS total_revenue FROM grouped GROUP BY phone_no, group_seq ) select combined_order_timestamp::date as order_date, (sum(total_revenue)/count(distinct combined_order_id))::int as aov from sessionized where combined_order_timestamp::date >= current_date-30 group by 1 );",
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
            