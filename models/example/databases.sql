{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table rosierfoods-wh.MapleMonk.membership_analysis as with membership_customers as (select customer_id, name, phone, email, sku, right(sku, length(sku) - 11) membership, min(order_time) membership_Start_date, DATE_ADD(min(order_time), INTERVAL cast(right(sku, length(sku) - 11) as int) MONTH) membership_end_date from rosierfoods-wh.MapleMonk.rosierfoods_wh_sales_consolidated where sku in (\'membership_3\', \'membership_6\', \'membership_12\') and order_status <> \'CANCELLED\' group by 1,2,3,4,5,6 ) select a.customer_id, b.membership_start_date, b.membership_end_date, b.name, b.email, b.phone, a.order_date, reference_Code, a.sku, a.product_name, order_Status, selling_price, discount, quantity, city, state, c.eligibility_flag from (select * from rosierfoods-wh.MapleMonk.rosierfoods_wh_sales_consolidated where ordeR_status <> \'CANCELLED\') a left join membership_customers b on a.customer_id = b.customer_id and a.order_time >= membership_start_date and a.ordeR_time <= membership_end_date left join ( select a.customer_id, case when b.membership_Start_date < \'2026-02-11\' and sum(selling_price) > 10000 then \'Eligible\' when b.membership_Start_date >= \'2026-02-11\' and sum(selling_price) > 15000 then \'Eligible\' else \'Not Eligible\' end eligibility_flag from (select * from rosierfoods-wh.MapleMonk.rosierfoods_wh_sales_consolidated where ordeR_status <> \'CANCELLED\') a left join (select * from membership_customers) b on a.customer_id = b.customer_id and a.order_time >= membership_start_date and a.ordeR_time <= membership_end_date where b.customer_id is not null group by 1, b.membership_start_Date ) c on a.customer_id = c.customer_id where b.customer_id is not null",
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
            