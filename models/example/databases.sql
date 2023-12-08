{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table bsc_db.maplemonk.bsc_db_affiliate_validation as select order_timestamp::date ordeR_Date ,order_name ,utm_source ,coalesce(b.ordeR_status,a.order_status) status ,discount_code ,sum(total_sales) total_Sales from bsc_db.maplemonk.bsc_db_shopify_fact_items a left join bsc_db.MAPLEMONK.bsc_db_Vinculum_fact_items b on a.ordeR_name = b.reference_code and a.line_item_id = b.SALEORDERITEMCODE group by 1,2,3,4,5",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from BSC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        