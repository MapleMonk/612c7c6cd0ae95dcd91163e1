{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table snitch_db.maplemonk.partner_snitch as select date, partner, order_name, email, customer_flag, sum(discount) discount, sum(selling_price) sales, sum(case when return_quantity <> 0 and cancelled_quantity = 0 then selling_price end) return_sales, sum(case when cancelled_quantity <> 0 then selling_price end) cancelled_sales, case when sum(return_quantity) > 0 and sum(return_quantity) < sum(suborder_quantity) then \'Partially Returned\' when sum(return_quantity) > 0 and sum(return_quantity) = sum(suborder_quantity) then \'Fully Returned\' when sum(return_quantity) = 0 then \'Not Returned\' end as return_status, case when sum(cancelled_quantity) > 0 and sum(cancelled_quantity) < sum(suborder_quantity) then \'Partially Cancelled\' when sum(cancelled_quantity) > 0 and sum(cancelled_quantity) = sum(suborder_quantity) then \'Fully Cancelled\' when sum(cancelled_quantity) = 0 then \'Not Cancelled\' end as cancelled_status from( select a.order_date date, a.order_id, b.order_name, b.partner, b.email, b.customer_flag, b.discount, a.suborder_quantity, a.selling_price, a.return_flag, a.cancelled_quantity, a.return_quantity from snitch_db.maplemonk.unicommerce_fact_items_snitch a left join (select order_name, line_item_id, case when final_utm_source like \'%FAMPAY%\' then \'FAMPAY\' when final_utm_source like \'%WISHLINK%\' then \'WISHLINK\' when upper(discount_code) = \'WISH20\' then \'WISHLINK\' when final_utm_source like \'%GRABON%\' then \'GRABON\' end as partner, customer_flag, email, discount from snitch_db.maplemonk.fact_items_snitch where partner in (\'FAMPAY\',\'WISHLINK\',\'GRABON\') ) b on a.order_name = b.order_name and b.line_item_id=split_part(a.saleorderitemcode,\'-\',0) where b.order_name is not null ) group by 1,2,3,4,5",
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
                        