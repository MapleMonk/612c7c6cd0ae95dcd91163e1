{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.izf_grn_fact_items as select cast(cast(po_id as float64) as int64) as po_id, cast(cast(grn_id as float64) as int64) as grn_id, po_ref_num, cast(cast(po_number as float64) as int64) po_number, po_detail_id, cp_id, grn_status, vendor_name, cast(left(po_created_date,10) as date) po_created_date, cast (left(grn_created_at,10) as date) grn_created_date, total_grn_value, inwarded_warehouse warehouse, sku, split(replace(UPPER(trim(parts[SAFE_OFFSET(ARRAY_LENGTH(parts) - 1)])),\'_\',\'\'),\'/Z\')[SAFE_OFFSET(0)] as sku_size, replace(replace(parts[safe_offset(0)],\'`\',\'\'),\'\\'\',\'\') sku_style, received_quantity from (select *, JSON_EXTRACT_SCALAR(json_string, \"$.sku\") as sku, JSON_EXTRACT_SCALAR(json_string, \"$.cp_id\") as cp_id, JSON_EXTRACT_SCALAR(json_string, \"$.purchase_order_detail_id\") as po_detail_id, cast(JSON_EXTRACT_SCALAR(json_string, \"$.received_quantity\") as float64) AS received_quantity, ARRAY( SELECT part FROM UNNEST(SPLIT(replace(JSON_EXTRACT_SCALAR(json_string, \"$.sku\"),\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts from maplemonk.easyecom_izf_grn_details, UNNEST(grn_items) AS json_string ) ; CREATE OR REPLACE TABLE izf-wh.maplemonk.izf_po_grn_fact_items as SELECT po_items.sku, po_items.sku_size AS sku_size, po_items.sku_style AS sku_style, po_items.product_description, po_items.cp_id, po_items.po_detail_id AS po_item_detail_id, po_items.po_created_warehouse, po_items.po_created_location_key, po_items.po_created_warehouse_c_id, po_items.po_number, po_items.po_created_date AS po_item_created_date, po_items.item_price, po_items.original_quantity, po_items.pending_quantity, grn_items.grn_created_date, grn_items.received_quantity, grn_items.grn_id, grn_items.po_ref_num, grn_items.grn_status, grn_items.vendor_name, grn_items.total_grn_value, grn_items.warehouse AS grn_warehouse, FROM izf-wh.maplemonk.izf_purchase_order_fact_items AS po_items LEFT JOIN maplemonk.izf_grn_fact_items AS grn_items ON po_items.po_number = grn_items.po_number AND po_items.po_detail_id = grn_items.po_detail_id AND po_items.sku = grn_items.sku AND po_items.cp_id = grn_items.cp_id AND CAST(po_items.po_created_date AS DATE) = grn_items.po_created_date;",
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
            