{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.izf_grn_fact_items as select po_id, grn_id, po_ref_num, grn_status, vendor_name, cast(left(po_created_date,10) as date) po_created_date, cast (left(grn_created_at,10) as date) grn_created_date, total_grn_value, inwarded_warehouse warehouse, sku, split(replace(UPPER(trim(parts[SAFE_OFFSET(ARRAY_LENGTH(parts) - 1)])),\'_\',\'\'),\'/Z\')[SAFE_OFFSET(0)] as sku_size, replace(replace(parts[safe_offset(0)],\'`\',\'\'),\'\\'\',\'\') sku_style, received_quantity from (select *, JSON_EXTRACT_SCALAR(json_string, \"$.sku\") as sku, cast(JSON_EXTRACT_SCALAR(json_string, \"$.received_quantity\") as float64) AS received_quantity, ARRAY( SELECT part FROM UNNEST(SPLIT(replace(JSON_EXTRACT_SCALAR(json_string, \"$.sku\"),\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts from maplemonk.easyecom_izf_grn_details, UNNEST(grn_items) AS json_string ) ;",
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
            