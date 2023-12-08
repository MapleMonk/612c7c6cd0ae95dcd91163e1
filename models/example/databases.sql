{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.stuck_orders as select * from snitch_db.maplemonk.unicommerce_fact_items_snitch where order_status in (\'PROCESSING\') and SHIPPING_STATUS in (\'PACKED\') and order_date <date(getdate())-2 and order_date >date(getdate())-90",
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
                        