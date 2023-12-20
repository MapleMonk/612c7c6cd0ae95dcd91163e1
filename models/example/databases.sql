{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.stuck_orders as select ufc.*, fc.payment_method from snitch_db.maplemonk.unicommerce_fact_items_snitch ufc left join snitch_db.maplemonk.fact_items_snitch fc on ufc.order_id = fc.order_id and fc.line_item_id=split_part(ufc.saleorderitemcode,\'-\',0) where ufc.order_status in (\'PROCESSING\') and SHIPPING_STATUS in (\'CREATED\', \'PACKED\') and order_date <date(getdate())-1 and order_date >date(getdate())-90",
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
                        