{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.order_funnel_snitch as select ga_date as date, ga_users as leads, ga_goal2completions + ga_goal4completions as addtocarts, ga_transactions as orders, ga_medium as medium, ga_source as source, ga_campaign as campaign from snitch_db.maplemonk.ga_snitch_order_funnel ; create or replace table snitch_db.maplemonk.orders_by_ga_source_snitch as select date, order_name, source, medium, campaign, phone, email, sum(suborder_quantity) quantity, sum(selling_price) sales, sum(return_quantity) return_quantity, sum(cancelled_quantity) cancelled_quantity, sum(case when return_quantity <> 0 then selling_price end) return_sales, sum(case when cancelled_quantity <> 0 then selling_price end) cancelled_sales, case when sum(return_quantity) > 0 and sum(return_quantity) < sum(suborder_quantity) then \'Partially Returned\' when sum(return_quantity) > 0 and sum(return_quantity) = sum(suborder_quantity) then \'Fully Returned\' when sum(return_quantity) = 0 then \'Not Returned\' end as return_status, case when sum(cancelled_quantity) > 0 and sum(cancelled_quantity) < sum(suborder_quantity) then \'Partially Cancelled\' when sum(cancelled_quantity) > 0 and sum(cancelled_quantity) = sum(suborder_quantity) then \'Fully Cancelled\' when sum(cancelled_quantity) = 0 then \'Not Cancelled\' end as cancelled_status from( select a.order_date date, a.order_id, b.order_name, b.email, b.phone, c.ga_source source, c.ga_medium medium, c.ga_campaign campaign, a.product_name, a.sku, a.suborder_quantity, a.selling_price, a.return_flag, a.cancelled_quantity, a.return_quantity, a.shippingpackagestatus from snitch_db.maplemonk.unicommerce_fact_items_snitch a left join (select distinct order_id, order_name, email,phone from snitch_db.maplemonk.fact_items_snitch) b on a.order_id = b.order_id left join snitch_db.maplemonk.ga_snitch_transactions_by_date c on b.order_name = c.ga_transactionId where a.order_Date::date > \'2023-02-01\'and order_name is not null ) group by 1,2,3,4,5,6,7 ;",
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
                        