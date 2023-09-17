{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.suspicious_orders as select date(order_timestamp) as dt, order_name, customer_id,customer_name, payment_gateway, sum(line_item_sales) as total_value from snitch_db.maplemonk.fact_items_snitch where dt >= date(getdate())-2 group by dt, order_name,customer_id,customer_name, payment_gateway order by total_value desc",
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
                        