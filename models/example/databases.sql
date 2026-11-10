{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table rosierfoods-wh.MapleMonk.second_purchase_analysis as with sales_data as ( select customer_id_final, acquisition_Date, acquisition_product, product_name, ordeR_date, dense_rank() over (partition by customer_id_final order by order_date asc) rw from rosierfoods-wh.MapleMonk.rosierfoods_wh_SALES_CONSOLIDATED WHERE customer_id_final IS NOT NULL AND LOWER(order_status) <> \'cancelled\' ) select * from sales_data where rw =2 ;",
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
            