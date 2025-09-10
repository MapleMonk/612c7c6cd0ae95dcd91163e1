{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE freakins-wh.MAPLEMONK.COHORT_ANALYSIS_FREAKINS AS WITH InitialResult AS ( SELECT customer_id_final, DATE_TRUNC(order_date, MONTH) AS order_month, SUM(IFNULL(selling_price, 0)) - IFNULL(SUM(IFNULL(tax, 0)), 0) AS Pre_sales FROM freakins-wh.MAPLEMONK.freakins_db_sales_consolidated WHERE lower(marketplace) like any (\'%shopify%\',\'%freakins%\',\'%website%\') and not(lower(order_status) like \'%cancel%\' and lower(oms_order_status) like \'%cancel%\' and lower(final_shipping_status) like \'%cancel%\') GROUP BY 1, 2 ), Start_Month AS ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id_final ORDER BY order_month ASC) AS rw FROM InitialResult ) WHERE rw = 1 ), combineResult AS ( SELECT ori.*, date1, Sales FROM Start_Month AS ori LEFT JOIN ( SELECT customer_id_final, order_month AS date1, Pre_sales AS Sales FROM InitialResult ) AS dup ON ori.customer_id_final = dup.customer_id_final ) SELECT c.*, cm.phone, cm.email, DATE_DIFF(date1, order_month, MONTH) AS next_month FROM combineResult AS c LEFT JOIN ( SELECT cid, phone, email FROM ( SELECT customer_id_final AS cid, phone, email, ROW_NUMBER() OVER (PARTITION BY customer_id_final ORDER BY 1) AS rw FROM freakins-wh.MAPLEMONK.freakins_db_sales_consolidated ) WHERE rw = 1 ) AS cm ON cm.cid = c.customer_id_final WHERE DATE_DIFF(date1, order_month, MONTH) >= 0;",
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
            