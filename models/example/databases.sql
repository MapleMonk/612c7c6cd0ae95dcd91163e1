{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `zouk-wh.STPL_Production_DB.SKU_level_WIP` AS SELECT sub_con_vendor_name, sub_con_order_number, item_id, item_name, product_category, estimated_quantity, quantity_produced, wip, execution_date, source, sort_order FROM ( SELECT sub_con_vendor_name, sub_con_order_number, item_id, item_name, product_category, estimated_quantity, quantity_produced, wip, execution_date, \'JW\' AS source, 1 AS sort_order FROM `zouk-wh.STPL_Production_DB.jw_process_details` UNION ALL SELECT supplier_name AS sub_con_vendor_name, po_number AS sub_con_order_number, item_id, item_description AS item_name, item_category AS product_category, po_quantity AS estimated_quantity, delivered_quantity AS quantity_produced, balance_quantity AS wip, document_date AS execution_date, \'OR\' AS source, 2 AS sort_order FROM `zouk-wh.STPL_Production_DB.or_purchase_orders` ) ORDER BY sort_order, execution_date;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            