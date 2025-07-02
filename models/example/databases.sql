{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.izf_grn_fact_items as select po_id, grn_id, po_ref_num, grn_status, vendor_name, cast(left(po_created_date,10) as date) po_created_date, cast (left(grn_created_at,10) as date) grn_created_date, total_grn_value, inwarded_warehouse warehouse, JSON_VALUE(parse_json(json_string), \"$.sku\") AS sku, cast(JSON_VALUE(parse_json(json_string), \"$.received_quantity\") as float64) AS received_quantity from maplemonk.easyecom_izf_grn_details, UNNEST(grn_items) AS json_string ;",
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
            