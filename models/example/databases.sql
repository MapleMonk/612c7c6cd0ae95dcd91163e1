{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.cod_cancel_orders as WITH CODCancelledOrders AS ( SELECT customer_id,customer_name, COUNT(distinct order_id) AS num_cod_orders FROM snitch_db.maplemonk.fact_items_snitch WHERE payment_method = \'COD\' AND order_status = \'CANCELLED\' AND order_timestamp >= CURRENT_DATE() - INTERVAL \'1 DAYS\' GROUP BY customer_id,customer_name ), CustomerOrders AS ( SELECT customer_id,customer_name, COUNT(distinct order_id) AS num_cancelled_orders FROM snitch_db.maplemonk.fact_items_snitch WHERE order_status = \'CANCELLED\' AND order_timestamp >= CURRENT_DATE() - INTERVAL \'1 DAYS\' GROUP BY customer_id,customer_name ) SELECT c.customer_id,c.customer_name, COALESCE(SUM(cod.num_cod_orders), 0) AS num_cod_orders, COALESCE(SUM(co.num_cancelled_orders), 0) AS num_cancelled_orders, CASE WHEN sum(cod.num_cod_orders) IS NOT NULL AND sum(co.num_cancelled_orders) IS NOT NULL THEN ROUND((sum(co.num_cancelled_orders) * 100.0) / sum(cod.num_cod_orders), 2) ELSE 0 END AS percentage_cancelled FROM ( SELECT DISTINCT customer_id,customer_name FROM snitch_db.maplemonk.fact_items_snitch WHERE order_timestamp >= CURRENT_DATE() - INTERVAL \'1 DAYS\' group by customer_id,customer_name ) AS c LEFT JOIN CODCancelledOrders AS cod ON c.customer_id = cod.customer_id LEFT JOIN CustomerOrders AS co ON c.customer_id = co.customer_id GROUP BY c.customer_id, c.customer_name ORDER BY num_cancelled_orders DESC",
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
                        