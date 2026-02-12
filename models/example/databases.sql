{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.grn_fact_items as SELECT po_id, grn_id, po_ref_num, grn_status, vendor_name, CAST(LEFT(po_created_date, 10) AS DATE) AS po_created_date, CAST(LEFT(grn_created_at, 10) AS DATE) AS grn_created_date, total_grn_value, inwarded_warehouse AS warehouse, JSON_EXTRACT_SCALAR(json_string, \"$.sku\") AS sku, CAST(JSON_EXTRACT_SCALAR(json_string, \"$.received_quantity\") AS FLOAT64) AS received_quantity FROM maplemonk.easyecom_saada_grn_details, UNNEST(grn_items) AS json_string; create or replace table maplemonk.inventory_snapshot_fact_items as select cast(left(report_generated_Date,10) as date) date, location, replace(sku,\'`\',\'\') sku, product_name, cast(Available_Quantity as float64) available_quantity, cast(qc_passed as float64) qc_passed, cast(qc_pending as float64) qc_pending, from maplemonk.easyecom_saada_inventory_snapshot ;",
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
            