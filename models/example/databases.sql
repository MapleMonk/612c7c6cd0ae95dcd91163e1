{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE neshanka-wh.maplemonk.neshanka_purchase_order_fact_items AS SELECT po_number, po_created_date, item_price, sku, vendor_code, vendor_name, product_description, cp_id, po_id, po_status, po_detail_id, original_quantity, pending_quantity, po_created_warehouse, po_created_location_key, po_created_warehouse_c_id, expected_delivery_date FROM ( SELECT cast(po_number as int64) as po_number, cast(po_created_date as datetime) as po_created_date, item_price, sku, po_id, product_description, cp_id, po_detail_id, original_quantity, pending_quantity, vendor_code, vendor_name, po_created_warehouse, po_created_location_key, po_created_warehouse_c_id, CASE cast(cast(po_status_id as float64) as int64) WHEN 1 THEN \'Open\' WHEN 2 THEN \'Waiting for Approval\' WHEN 3 THEN \'Approved\' WHEN 4 THEN \'Rejected\' WHEN 5 THEN \'Completed\' WHEN 6 THEN \'Pending on Supplier\' WHEN 7 THEN \'cancelled\' WHEN 8 THEN \'Payment Pending\' WHEN 9 THEN \'Payment Done\' WHEN 11 THEN \'Shipped to FF\' WHEN 12 THEN \'Pending Dispatch on FF\' WHEN 13 THEN \'Shipped\' WHEN 14 THEN \'Shipped by FF\' WHEN 15 THEN \'Received by FF\' WHEN 16 THEN \'Invoice done by Vendor\' ELSE \'Unknown Status\' END AS po_status, ROW_NUMBER() OVER (PARTITION BY cast(po_number as int64), cast(po_created_date as datetime),sku ORDER BY cast(po_created_date as datetime) DESC, po_detail_id DESC) AS rw, expected_delivery_date FROM ( SELECT *, cast(json_extract_scalar(po,\'$.item_price\')as float64) as item_price, json_extract_scalar(po,\'$.sku\') as sku, json_extract_scalar(po,\'$.product_description\') as product_description, json_extract_scalar(po,\'$.cp_id\') as cp_id, json_extract_scalar(po,\'$.purchase_order_detail_id\') as po_detail_id, cast(json_extract_scalar(po,\'$.original_quantity\') as float64) as original_quantity, cast(json_extract_scalar(po,\'$.pending_quantity\') as float64) as pending_quantity, ARRAY(SELECT part FROM UNNEST(SPLIT(replace(coalesce(json_extract_scalar(po,\'$.sku\')),\'_\',\'-\'), \'-\')) AS part WHERE part != \'\' ) as parts FROM neshanka-wh.`MapleMonk.easyecom_purchase_orders` LEFT JOIN unnest(po_items) po ) ) WHERE rw = 1;",
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
            