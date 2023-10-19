{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.customer_support_cust_details as select customer_id, order_id, order_name, phone, email, customer_name from snitch_db.maplemonk.fact_items_snitch group by customer_id, order_id, order_name, phone, email, customer_name",
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
                        