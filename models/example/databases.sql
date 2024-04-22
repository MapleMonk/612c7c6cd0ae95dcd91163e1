{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table italiancolony_db.maplemonk.forward_shipment_reconciliation as select order_date::Date order_Date, reference_code ordeR_name, a.awb, sku, dispatch_date::Date dispatch_date, b.current_status clickpost_status, getdate()::Date - dispatch_date::Date days_since_dispatch from italiancolony_db.MAPLEMONK.italiancolony_db_unicommerce_fact_items a left join (select awb_number ,current_status from italiancolony_db.MAPLEMONK.italiancolony_DB_CLICKPOST_FACT_ITEMS )b on a.awb = b.awb_number where current_status not in (\'RTO-Delivered\',\'Delivered\') and getdate()::Date - dispatch_date::Date >=15 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from italiancolony_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        