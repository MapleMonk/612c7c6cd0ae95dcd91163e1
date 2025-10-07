{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE freakins-wh.MAPLEMONK.COHORT_ANALYSIS_FREAKINS AS WITH customers AS ( SELECT customer_id_final, DATE_TRUNC(first_complete_order_date, MONTH) AS order_month, DATE_TRUNC(order_timestamp, MONTH) AS date1, sum(ifnull(line_item_Sales,0)) - sum(ifnull(item_refund_amount,0)) AS Sales, FROM freakins-wh.MAPLEMONK.freakins_db_shopify_fact_items WHERE not(lower(order_status) like \'%cancel%\' and lower(shipping_status) like \'%cancel%\') and not(lower(order_name) like \'%e%\') and customer_id_final IS NOT NULL and pragma_return_flag is null GROUP BY 1,2,3 ) SELECT c.*, cm.phone, cm.email, DATE_DIFF(date1, order_month, MONTH) AS next_month, 0.80 as gross_margin, mc.spend FROM customers AS c LEFT JOIN ( SELECT cid, phone, email FROM ( SELECT customer_id_final AS cid, phone, email, ROW_NUMBER() OVER (PARTITION BY customer_id_final ORDER BY 1) AS rw FROM freakins-wh.MAPLEMONK.freakins_db_shopify_fact_items ) WHERE rw = 1 ) AS cm ON cm.cid = c.customer_id_final LEFT JOIN (select date_trunc(date,month) as spend_month, sum(ifnull(Spend,0)) as spend from maplemonk.freakins_Db_marketing_consolidated group by 1) mc ON mc.spend_month = c.order_month WHERE DATE_DIFF(date1, order_month, MONTH) >= 0;",
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
            