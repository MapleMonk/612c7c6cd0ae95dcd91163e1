{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_Customer_Journey_Analysis` as WITH base AS ( SELECT customer_id, DATE(order_timestamp) AS order_date, ORDER_NAME AS order_name, Product_Category, Collection, TOTAL_SALES FROM `MapleMonk.zouk_SHOPIFY_FACT_ITEMS` WHERE LOWER(order_status) NOT LIKE \'%cancel%\' and lower(marketplace) not like \'%pos%\' AND customer_id IS NOT NULL ), customer_day AS ( SELECT customer_id, order_date, COUNT(DISTINCT order_name) AS orders_that_day, SUM(TOTAL_SALES) AS sales_that_day, STRING_AGG(DISTINCT Product_Category, \', \') AS Category_that_day, STRING_AGG(DISTINCT Collection, \', \') AS collection_that_day FROM base GROUP BY customer_id, order_date ), sequenced AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS step_seq, MAX(order_date) OVER (PARTITION BY customer_id) AS last_order_date FROM customer_day ), customer_summary AS ( SELECT customer_id, MIN(order_date) AS first_order_date, MAX(order_date) AS recent_order_date, MAX(step_seq) AS total_purchase_days, STRING_AGG(DISTINCT collection_that_day, \', \') AS all_collection_purchased, ARRAY_AGG(collection_that_day ORDER BY order_date DESC LIMIT 1)[OFFSET(0)] AS recent_collection, STRING_AGG( category_that_day, \' → \' ORDER BY order_date ) AS journey_summary, SUM(IF(order_date < last_order_date, sales_that_day, 0)) AS ltv_excl_recent, SAFE_DIVIDE( SUM(IF(order_date < last_order_date, sales_that_day, 0)), SUM(IF(order_date < last_order_date, orders_that_day, 0)) ) AS aov_excl_recent, SUM(sales_that_day) AS total_lifetime_sales, SUM(orders_that_day) AS total_lifetime_orders FROM sequenced GROUP BY customer_id ) SELECT customer_id, first_order_date, recent_order_date, recent_collection, total_purchase_days, all_collection_purchased, journey_summary, ROUND(ltv_excl_recent, 2) AS ltv, ROUND(aov_excl_recent, 2) AS aov, total_lifetime_sales, total_lifetime_orders FROM customer_summary WHERE total_purchase_days >= 2;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            