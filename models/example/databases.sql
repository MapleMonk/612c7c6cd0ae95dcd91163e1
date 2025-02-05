{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_affiliate_validation as select order_name , order_timestamp::Date order_Date , final_affiliate affiliate , trackier_affiliate , b.current_Status , a.final_utm_channel , a.discount_code , sum(total_sales) total_sales , sum(case when lower(coalesce(b.current_status, a.order_status)) = \'cancelled\' then total_sales end) cancelled_sales , sum(case when lower(coalesce(b.current_status, a.order_status)) <> \'cancelled\' and lower(tags) like \'%return%\' then total_sales end) returned_sales , sum(total_sales) - ifnull(sum(case when lower(coalesce(b.current_status, a.order_status)) = \'cancelled\' then total_sales end),0) - ifnull(sum(case when lower(coalesce(b.current_status, a.order_status)) <> \'cancelled\' and lower(tags) like \'%return%\' then total_sales end),0) net_sales from prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS a left join prd_db.justherbs.dwh_CLICKPOST_FACT_ITEMS b on a.awb = b.awb_number where final_Affiliate is not null group by 1,2,3,4,5,6,7 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            