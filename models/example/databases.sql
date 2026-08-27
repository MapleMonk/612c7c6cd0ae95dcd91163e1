{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table thd-pharma-wh.maplemonk.ozone_s3_myntra_sjit_40_columns_historical_fact_items as Select DATE(SAFE_CAST(NULLIF(TRIM(order_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS order_date, order_release_id, seller_order_id, seller_sku_code, hsn_code, brand, article_type, gender, order_status, cast(MRP as FLOAT64) as MRP, cast(customer_paid_amount as FLOAT64) as customer_paid_amount, cast(seller_discount as FLOAT64) as seller_discount, cast(seller_product_amount_postpaid as FLOAT64) as seller_product_amount_postpaid, cast(seller_product_amount_prepaid as FLOAT64) as seller_product_amount_prepaid, cast(platform_discount as FLOAT64) as platform_discount, cast(commission_pct_incl_gst as FLOAT64) as commission_pct_incl_gst, cast(commission_amount_incl_gst_postpaid as FLOAT64) as commission_amount_incl_gst_postpaid, cast(commission_amount_incl_gst_prepaid as FLOAT64) as commission_amount_incl_gst_prepaid, product_tax_category, cast(cgst_amount as FLOAT64) as cgst_amount, cast(igst_amount as FLOAT64) as igst_amount, cast(sgst_amount as FLOAT64) as sgst_amount, cast(tcs_amount_postpaid as FLOAT64) as tcs_amount_postpaid, cast(tds_amount_postpaid as FLOAT64) as tds_amount_postpaid, cast(tcs_amount_prepaid as FLOAT64) as tcs_amount_prepaid, cast(tds_amount_prepaid as FLOAT64) as tds_amount_prepaid, DATE(SAFE_CAST(NULLIF(TRIM(packing_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS packing_date, DATE(SAFE_CAST(NULLIF(TRIM(pickup_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS pickup_date, DATE(SAFE_CAST(NULLIF(TRIM(delivery_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS delivery_date, DATE(SAFE_CAST(NULLIF(TRIM(return_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS return_date, return_type, warehouse_id, warehouse_state, invoice_number, packet_id, tracking_no_fwd, tracking_no_reverse, shipment_zone_classification, delivery_pin_code, shipping_state from thd-pharma-wh.maplemonk.ozone_s3_historic_myntra_sjit_40_columns;",
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
            