{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.canellation_uc_api as (select order_id as \"Sale Order Code\", saleorderitemcode as \"Sale Order Item Code\", warehouse_name as \"Facility Code\", \'Order Error\' as Reason, \'TRUE\' as \"Cancel On Channel\" from snitch_db.maplemonk.warehouse_sla_performance where marketplace_mapped in (\'STORE\',\'FLIPKART_SOR\',\'SAPL_WH_TRANSFERS\',\'MYNTRA_SJIT\',\'SLIKK\',\'SNITCH_HQ\',\'KNOT\',\'B2B CORPORATE CHANNEL\',\'M-NOW\') and item_status = \'UNFULFILLABLE\' and timestampdiff(minute,uc_created,shipping_last_update_timestamp) < 30) UNION ALL (select order_id as \"Sale Order Code\", saleorderitemcode as \"Sale Order Item Code\", warehouse_name as \"Facility Code\", \'Not Found\' as Reason, \'TRUE\' as \"Cancel On Channel\" from snitch_db.maplemonk.warehouse_sla_performance where marketplace_mapped in (\'STORE\',\'FLIPKART_SOR\',\'SAPL_WH_TRANSFERS\',\'MYNTRA_SJIT\',\'SLIKK\',\'SNITCH_HQ\',\'KNOT\',\'B2B CORPORATE CHANNEL\',\'M-NOW\') and item_status = \'UNFULFILLABLE\' and timestampdiff(minute,uc_created,shipping_last_update_timestamp) >= 30)",
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
            