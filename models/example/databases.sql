{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.TEMPORARY_EXCHANGE_ORDERS AS with details as ( select right(note,7) as original_order , concat(original_order,sku_group) as i2, tags, order_name as new_order, sku_group from snitch_db.maplemonk.fact_items_snitch where (tags like \'%eco%\' or tags like \'%exchange%\' or tags like \'%RMS-Exchange%\') and note like \'%Original Order No%\' and order_timestamp::DATE >= \'2025-10-01\' ), ex as ( select concat(order_id,REGEXP_SUBSTR(sku_list, \'^[^-]+-[^-]+\')) as i1 , pickup_date, awb_number from snitch_db.maplemonk.cp_1 where mode = \'Reverse\' ) , new as ( select sku, order_status, order_timestamp, awb as ex_awb, shipping_last_update_timestamp, sla_status , item_status, created_timestamp, order_name from snitch_db.maplemonk.warehouse_sla_performance where marketplace_mapped = \'SHOPIFY\' ), final1 as ( select d.*,ex.pickup_date, ex.awb_number , n.* from details d left join ex on d.i2 = ex.i1 left join new n on d.new_order = n.order_name ) select * from final1 where order_status = \'PENDING_VERIFICATION\' and pickup_date is not null",
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
            