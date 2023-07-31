{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.wishlink_orders_snitch as SELECT a.order_date, a.order_id, b.order_name, b.phone, b.email, b.customer_flag, SUM(b.discount) AS discount, SUM(suborder_quantity) AS quantity, SUM(selling_price) AS sales, SUM(CASE WHEN a.return_quantity <> 0 AND cancelled_quantity = 0 THEN selling_price END) AS return_sales, SUM(CASE WHEN cancelled_quantity <> 0 THEN selling_price END) AS cancelled_sales, CASE WHEN SUM(a.return_quantity) > 0 AND SUM(a.return_quantity) < SUM(suborder_quantity) THEN \'Partially Returned\' WHEN SUM(a.return_quantity) > 0 AND SUM(a.return_quantity) = SUM(suborder_quantity) THEN \'Fully Returned\' WHEN SUM(a.return_quantity) = 0 THEN \'Not Returned\' END AS return_status, CASE WHEN SUM(cancelled_quantity) > 0 AND SUM(cancelled_quantity) < SUM(suborder_quantity) THEN \'Partially Cancelled\' WHEN SUM(cancelled_quantity) > 0 AND SUM(cancelled_quantity) = SUM(suborder_quantity) THEN \'Fully Cancelled\' WHEN SUM(cancelled_quantity) = 0 THEN \'Not Cancelled\' END AS cancelled_status FROM snitch_db.maplemonk.fact_items_snitch b LEFT JOIN ( SELECT order_date, order_id, suborder_quantity, selling_price, return_quantity, cancelled_quantity, saleorderitemcode FROM snitch_db.maplemonk.unicommerce_fact_items_snitch) a ON a.order_id = b.order_id and b.line_item_id=split_part(a.saleorderitemcode,\'-\',0) WHERE a.order_id IN ( SELECT DISTINCT order_id FROM snitch_db.maplemonk.fact_items_snitch WHERE order_name IN ( SELECT DISTINCT order_id FROM snitch_db.maplemonk.affiliates_wishlink ) ) GROUP BY 1,2,3,4,5,6 order by sales desc",
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
                        