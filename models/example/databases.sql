{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.tekkitake_amazon_returns_fact_items as select ASIN, return_type, order_id, tracking_id as awb, merchant_SKU, item_name as returned_product_name, invoice_number, case when trim(order_Date) = \'\' then null else format_date(\'%Y-%m-%d\', parse_date(\'%d-%b-%Y\', trim(order_Date))) end as order_date, safe_cast(order_amount as float64) as selling_price, return_reason, case when trim(return_request_date) = \'\' then null else format_date(\'%Y-%m-%d\', parse_date(\'%d-%b-%Y\',nullif(trim(return_request_date), \'\'))) end as return_requested_at, return_request_status as return_approval_status, cast(return_quantity as int64) as return_quantity, safe_cast(refunded_amount as float64) as refunded_amount, case when trim(return_delivery_date) = \'\' then null else format_date(\'%Y-%m-%d\', parse_date(\'%d-%b-%Y\', return_delivery_date)) end as return_delivery_date, return_carrier as shipping_courier, resolution, case when (lower(resolution) like \'%exchange%\' or lower(resolution) like \'%replacement%\') then \'exchange\' when (lower(return_type) like \'%rejected%\' or lower(return_type) like \'%undelivered%\') then \'RTO\' else \'return\' end as return_request_type FROM `MAPLEMONK.TEKKITAKE_GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE` r ;",
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
            