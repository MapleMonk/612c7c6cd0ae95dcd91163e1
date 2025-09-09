{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.izf_grn_fact_items as select cast(cast(po_id as float64) as int64) as po_id, cast(cast(grn_id as float64) as int64) as grn_id, po_ref_num, cast(cast(po_number as float64) as int64) po_number, po_detail_id, cp_id, grn_status, vendor_name, cast(left(po_created_date,10) as date) po_created_date, cast (left(grn_created_at,10) as date) grn_created_date, total_grn_value, inwarded_warehouse warehouse, sku, split(replace(UPPER(trim(parts[SAFE_OFFSET(ARRAY_LENGTH(parts) - 1)])),\'_\',\'\'),\'/Z\')[SAFE_OFFSET(0)] as sku_size, replace(replace(parts[safe_offset(0)],\'`\',\'\'),\'\\'\',\'\') sku_style, sum(received_quantity) received_quantity from (select *, JSON_EXTRACT_SCALAR(json_string, \"$.sku\") as sku, JSON_EXTRACT_SCALAR(json_string, \"$.cp_id\") as cp_id, JSON_EXTRACT_SCALAR(json_string, \"$.purchase_order_detail_id\") as po_detail_id, cast(JSON_EXTRACT_SCALAR(json_string, \"$.received_quantity\") as float64) AS received_quantity, ARRAY( SELECT part FROM UNNEST(SPLIT(replace(JSON_EXTRACT_SCALAR(json_string, \"$.sku\"),\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts from maplemonk.easyecom_izf_grn_details, UNNEST(grn_items) AS json_string ) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 ; CREATE OR REPLACE TABLE izf-wh.maplemonk.izf_po_grn_fact_items as SELECT po.sku, po.sku_size AS sku_size, po.sku_style AS sku_style, po.product_description, po.cp_id, po.po_id, po.po_status, po.po_detail_id AS po_item_detail_id, po.po_created_warehouse, po.po_created_location_key, po.po_created_warehouse_c_id, po.po_number, po.po_created_date AS po_item_created_date, po.item_price, po.original_quantity, po.pending_quantity, grn.grn_created_date, grn.received_quantity, grn.grn_id, grn.po_ref_num, grn.grn_status, grn.vendor_name, grn.total_grn_value, grn.warehouse AS grn_warehouse, lt.days_to_delivery FROM izf-wh.maplemonk.izf_purchase_order_fact_items AS po LEFT JOIN maplemonk.izf_grn_fact_items AS grn ON po.po_number = grn.po_number AND CAST(po.po_created_date AS DATE) = grn.po_created_date AND po.po_detail_id = grn.po_detail_id AND po.sku = grn.sku AND po.cp_id = grn.cp_id LEFT JOIN ( SELECT REPLACE(REPLACE(lower(TRIM(REPLACE(style, \' \', \'\'))), \'men\', \'\'), \'m\', \'\') AS style, days_to_delivery FROM maplemonk.izf_po_style_lead_time QUALIFY row_number() OVER (PARTITION BY REPLACE(REPLACE(lower(TRIM(REPLACE(style, \' \', \'\'))), \'men\', \'\'), \'m\', \'\') ORDER BY 1) = 1 ) AS lt ON lt.style = REPLACE(REPLACE(lower(po.sku_style), \'men\', \'\'), \'m\', \'\') ;",
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
            