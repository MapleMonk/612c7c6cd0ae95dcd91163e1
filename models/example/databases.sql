{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_repeat_purchase_behaviour as select n.first_order, n.first_order_product, n.first_order_category, m.subsequent_order, m.subsequent_product, m.subsequent_category,rw ordeR_number, o.* from (select customer_id_final,ordeR_name subsequent_order, rw, product_name_final subsequent_product, product_category subsequent_category from (select customer_id_final,reference_code order_name, product_name_final, product_category, dense_rank() over (partition by customer_id_final order by order_timestamp) rw from prd_db.justherbs.dwh_sales_consolidated where ordeR_status <> \'CANCELLED\' and marketplace = \'SHOPIFY_JUSTHERBS\' ) ) m left join ( select a.customer_id_final, reference_code first_order, product_name_final first_ordeR_product, product_category first_ordeR_category from prd_db.justherbs.dwh_sales_consolidated a left join (select customer_id_final, min(ordeR_timestamp) first_order_time from prd_db.justherbs.dwh_sales_consolidated where ordeR_status <> \'CANCELLED\' and marketplace = \'SHOPIFY_JUSTHERBS\' group by 1 )b on a.customer_id_final = b.customer_id_final and a.ordeR_timestamp = b.first_order_time where b.first_order_time is not null and ordeR_status <> \'CANCELLED\' and a.marketplace = \'SHOPIFY_JUSTHERBS\' ) n on m.customer_id_final = n.customer_id_final left join prd_db.justherbs.dwh_CUSTOMER_MASTER o on o.mm_customer_id = m.customer_id_final ;",
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
            