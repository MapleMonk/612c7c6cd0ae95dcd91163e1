{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.suspicious_orders as select date(order_timestamp) as dt, order_name, customer_id, customer_name, payment_gateway, payment_method, checkout, bnpl_flag, source, sum(line_item_sales) as line_item_total, sum(net_sales) as net_total, sum(gross_sales) as gross_total from snitch_db.maplemonk.fact_items_snitch where dt >= date(getdate())-2 group by 1,2,3,4,5,6,7,8,9 order by line_item_total desc",
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
                        