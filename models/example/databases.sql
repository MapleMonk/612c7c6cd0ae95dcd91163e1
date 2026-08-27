{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table thd-pharma-wh.maplemonk.ozone_s3_myntra_sjit_45_columns_historical_fact_items as Select DATE(SAFE_CAST(NULLIF(TRIM(order_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS order_date, order_release_id, seller_order_id, seller_sku_code, hsn_code, brand, article_type, gender, order_status, SAFE_CAST(NULLIF(TRIM(MRP), \'\') AS FLOAT64) as MRP, SAFE_CAST(NULLIF(TRIM(customer_paid_amount), \'\') AS FLOAT64) as customer_paid_amount, SAFE_CAST(NULLIF(TRIM(seller_discount), \'\') AS FLOAT64) as seller_discount, SAFE_CAST(NULLIF(TRIM(seller_product_amount_postpaid), \'\') AS FLOAT64) as seller_product_amount_postpaid, SAFE_CAST(NULLIF(TRIM(seller_product_amount_prepaid), \'\') AS FLOAT64) as seller_product_amount_prepaid, SAFE_CAST(NULLIF(TRIM(platform_discount), \'\') AS FLOAT64) as platform_discount, SAFE_CAST(NULLIF(TRIM(commission_pct_incl_gst), \'\') AS FLOAT64) as commission_pct_incl_gst, SAFE_CAST(NULLIF(TRIM(commission_amount_incl_gst_postpaid), \'\') AS FLOAT64) as commission_amount_incl_gst_postpaid, SAFE_CAST(NULLIF(TRIM(commission_amount_incl_gst_prepaid), \'\') AS FLOAT64) as commission_amount_incl_gst_prepaid, product_tax_category, SAFE_CAST(NULLIF(TRIM(cgst_amount), \'\') AS FLOAT64) as cgst_amount, SAFE_CAST(NULLIF(TRIM(igst_amount), \'\') AS FLOAT64) as igst_amount, SAFE_CAST(NULLIF(TRIM(sgst_amount), \'\') AS FLOAT64) as sgst_amount, SAFE_CAST(NULLIF(TRIM(tcs_amount_postpaid), \'\') AS FLOAT64) as tcs_amount_postpaid, SAFE_CAST(NULLIF(TRIM(tds_amount_postpaid), \'\') AS FLOAT64) as tds_amount_postpaid, SAFE_CAST(NULLIF(TRIM(tcs_amount_prepaid), \'\') AS FLOAT64) as tcs_amount_prepaid, SAFE_CAST(NULLIF(TRIM(tds_amount_prepaid), \'\') AS FLOAT64) as tds_amount_prepaid, DATE(SAFE_CAST(NULLIF(TRIM(packing_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS packing_date, DATE(SAFE_CAST(NULLIF(TRIM(pickup_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS pickup_date, DATE(SAFE_CAST(NULLIF(TRIM(delivery_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS delivery_date, DATE(SAFE_CAST(NULLIF(TRIM(return_date), \'\') AS TIMESTAMP), \'Asia/Kolkata\') AS return_date, return_type, warehouse_id, warehouse_state, invoice_number, packet_id, tracking_no_fwd, tracking_no_reverse, shipment_zone_classification, delivery_pin_code, shipping_state, customer_state, gst_state, SAFE_CAST(NULLIF(TRIM(sale_igst_rate), \'\') AS FLOAT64) AS sale_igst_rate, SAFE_CAST(NULLIF(TRIM(sale_cgst_rate), \'\') AS FLOAT64) AS sale_cgst_rate, SAFE_CAST(NULLIF(TRIM(sale_sgst_rate), \'\') AS FLOAT64) AS sale_sgst_rate from thd-pharma-wh.maplemonk.ozone_s3_historic_myntra_sjit_45_columns;",
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
            