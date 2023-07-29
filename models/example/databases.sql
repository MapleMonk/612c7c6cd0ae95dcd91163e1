{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.wishlink_orders_snitch as select a.order_date, a.order_id, b.order_name, b.phone, b.email, a.shippingpackagestatus, b.customer_flag, sum(b.discount) discount, sum(suborder_quantity) quantity, sum(selling_price) sales, sum(case when return_quantity <> 0 and cancelled_quantity = 0 then selling_price end) return_sales, sum(case when cancelled_quantity <> 0 then selling_price end) cancelled_sales, case when sum(return_quantity) > 0 and sum(return_quantity) < sum(suborder_quantity) then \'Partially Returned\' when sum(return_quantity) > 0 and sum(return_quantity) = sum(suborder_quantity) then \'Fully Returned\' when sum(return_quantity) = 0 then \'Not Returned\' end as return_status, case when sum(cancelled_quantity) > 0 and sum(cancelled_quantity) < sum(suborder_quantity) then \'Partially Cancelled\' when sum(cancelled_quantity) > 0 and sum(cancelled_quantity) = sum(suborder_quantity) then \'Fully Cancelled\' when sum(cancelled_quantity) = 0 then \'Not Cancelled\' end as cancelled_status from snitch_db.maplemonk.unicommerce_fact_items_snitch a left join (select distinct order_id, order_name, phone, email,discount, customer_flag from snitch_db.maplemonk.fact_items_snitch) b on a.order_id = b.order_id where a.order_id in (select distinct order_id from snitch_db.maplemonk.fact_items_snitch where order_name in (select distinct order_id from snitch_db.maplemonk.affiliates_wishlink)) group by 1,2,3,4,5,6,7",
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
                        