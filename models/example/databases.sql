{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table izf-wh.maplemonk.izf_purchase_order_fact_items as select cast(po_number as int64) as po_number, cast(po_created_date as datetime) as po_created_date, item_price, sku, product_description, cp_id, po_detail_id, original_quantity, pending_quantity, split(replace(UPPER(trim(parts[SAFE_OFFSET(ARRAY_LENGTH(parts) - 1)])),\'_\',\'\'),\'/Z\')[SAFE_OFFSET(0)] as sku_size, replace(replace(parts[safe_offset(0)],\'`\',\'\'),\'\\'\',\'\') sku_style, po_created_warehouse, po_created_location_key, po_created_warehouse_c_id from (select *, cast(json_extract_scalar(po,\'$.item_price\')as float64) as item_price, json_extract_scalar(po,\'$.sku\') as sku, json_extract_scalar(po,\'$.product_description\') as product_description, json_extract_scalar(po,\'$.cp_id\') as cp_id, json_extract_scalar(po,\'$.purchase_order_detail_id\') as po_detail_id, cast(json_extract_scalar(po,\'$.original_quantity\') as float64) as original_quantity, cast(json_extract_scalar(po,\'$.pending_quantity\') as float64) as pending_quantity, ARRAY(SELECT part FROM UNNEST(SPLIT(replace(coalesce(json_extract_scalar(po,\'$.sku\')),\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts from izf-wh.`MapleMonk.easyecom_izf_purchase_orders` left join unnest(po_items) po );",
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
            