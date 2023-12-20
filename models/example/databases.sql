{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table offduty_db.maplemonk.offduty_Db_shopify_returns_fact_items as select order_number ,sku ,reason ,status ,exchange_order_name ,exchange_with_sku ,item_quantity ,serial_number ,case when lower(serial_number) like \'%ret%\' then 1 else 0 end rp_return_flag ,try_cast(eligible_refund_amount as float) refund_amount ,case when lower(serial_number) like \'%ret%\' then \'CUSTOMER RETURNED\' else \'EXCHANGED\' end order_status from offduty_db.maplemonk.returns_reason_data;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from offduty_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        