{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE store_nps_responses AS SELECT t2.store_code, t3.store_name, CAST(t2.mem_id AS NUMERIC) AS mem_id, CAST(t1.nps_score AS NUMERIC) AS nps_score, CAST(t1.store_experience_rating AS NUMERIC) AS store_experience_rating, t3.region, t3.cluster, t3.party, t4.AM, TO_DATE(TO_TIMESTAMP_NTZ(created_at)) AS created_at_date, TO_DATE(TO_TIMESTAMP_NTZ(order_created_date)) AS order_created_at_date, t1.bill_no, t1.product_ids, t1.trigger_event, t1.free_text_feedback FROM offline_nps_offline_nps t1 LEFT JOIN retail_orders_summary_retail_order_summary t2 ON t1.bill_no = t2.bill_no LEFT JOIN offline_store_detailed_mapping t3 ON t2.store_code = t3.store_code LEFT JOIN master_offine t4 ON t3.store_code = t4.\"BRANCH CODE\";",
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
            