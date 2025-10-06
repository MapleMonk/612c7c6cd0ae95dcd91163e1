{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE freakins-wh.MAPLEMONK.COHORT_ANALYSIS_FREAKINS AS WITH InitialResult AS ( SELECT customer_id_final, DATE_TRUNC(order_timestamp, MONTH) AS order_month, sum(ifnull(line_item_Sales,0)) - sum(ifnull(item_refund_amount,0)) AS Pre_sales FROM freakins-wh.MAPLEMONK.freakins_db_shopify_fact_items WHERE lower(marketplace) like any (\'%shopify%\',\'%freakins%\',\'%website%\') and not(lower(order_status) like \'%cancel%\' and lower(shipping_status) like \'%cancel%\') and not(lower(order_name) like \'%e%\') GROUP BY 1,2 ), Start_Month AS ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id_final ORDER BY order_month ASC) AS rw FROM InitialResult ) WHERE rw = 1 ), combineResult AS ( SELECT ori.*, date1, Sales FROM Start_Month AS ori LEFT JOIN ( SELECT customer_id_final, order_month AS date1, Pre_sales AS Sales FROM InitialResult ) AS dup ON ori.customer_id_final = dup.customer_id_final ) SELECT c.*, cm.phone, cm.email, DATE_DIFF(date1, order_month, MONTH) AS next_month, 0.80 as gross_margin, mc.spend FROM combineResult AS c LEFT JOIN ( SELECT cid, phone, email FROM ( SELECT customer_id_final AS cid, phone, email, ROW_NUMBER() OVER (PARTITION BY customer_id_final ORDER BY 1) AS rw FROM freakins-wh.MAPLEMONK.freakins_db_shopify_fact_items ) WHERE rw = 1 ) AS cm ON cm.cid = c.customer_id_final LEFT JOIN (select date_trunc(date,month) as spend_month, sum(ifnull(Spend,0)) as spend from maplemonk.freakins_Db_marketing_consolidated group by 1) mc ON mc.spend_month = c.order_month WHERE DATE_DIFF(date1, order_month, MONTH) >= 0;",
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
            