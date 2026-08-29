{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.canellation_uc_api as (select order_id as \"Sale Order Code\", saleorderitemcode as \"Sale Order Item Code\", warehouse_name as \"Facility Code\", \'Order Error\' as Reason, \'TRUE\' as \"Cancel On Channel\" from snitch_db.maplemonk.warehouse_sla_performance where marketplace_mapped in (\'STORE\',\'FLIPKART_SOR\',\'SAPL_WH_TRANSFERS\',\'MYNTRA_SJIT\',\'SLIKK\',\'SNITCH_HQ\',\'KNOT\',\'B2B CORPORATE CHANNEL\',\'M-NOW\',\'INSTAMART_GURUGRAM\',\'INSTAMART_BLR\') and marketplace not like \'%B2B%\' and item_status = \'UNFULFILLABLE\' and timestampdiff(minute,uc_created,shipping_last_update_timestamp) < 30) UNION ALL (select order_id as \"Sale Order Code\", saleorderitemcode as \"Sale Order Item Code\", warehouse_name as \"Facility Code\", \'Not Found\' as Reason, \'TRUE\' as \"Cancel On Channel\" from snitch_db.maplemonk.warehouse_sla_performance where marketplace_mapped in (\'STORE\',\'FLIPKART_SOR\',\'SAPL_WH_TRANSFERS\',\'MYNTRA_SJIT\',\'SLIKK\',\'SNITCH_HQ\',\'KNOT\',\'B2B CORPORATE CHANNEL\',\'M-NOW\',\'INSTAMART_GURUGRAM\',\'INSTAMART_BLR\') and marketplace not like \'%B2B%\' and item_status = \'UNFULFILLABLE\' and timestampdiff(minute,uc_created,shipping_last_update_timestamp) >= 30) UNION ALL ( select order_id as \"Sale Order Code\", saleorderitemcode as \"Sale Order Item Code\", warehouse_name as \"Facility Code\", \'Post Dispatch\' as Reason, \'TRUE\' as \"Cancel On Channel\" from snitch_db.maplemonk.warehouse_b2b_performance where final_status like \'%POST-DISP%\' and marketplace_mapped in (\'STORE\') and marketplace not like \'%FLIP%\' and status not in (\'RTS-LOGIC\') and status not like \'%RTS%\' and item_status not in (\'PICKED\',\'PACKED\',\'READY_TO_SHIP\',\'DISPATCHED\',\'CANCELLED\',\'DELIVERED\') );",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            