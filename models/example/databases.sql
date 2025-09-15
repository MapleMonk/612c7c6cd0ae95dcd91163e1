{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.freakins_returns_data as SELECT \'WEBSITE\' as marketplace, cast(PARSE_TIMESTAMP(\'%Y-%m-%dT%H:%M:%E*S%Ez\', created_at) as datetime) created_at, order_name, JSON_VALUE(item, \'$.product_sku\') AS sku, JSON_VALUE(item, \'$.line_item_id\') AS line_item_id, JSON_VALUE(item, \'$.customer_reason_label\') AS customer_reason_label, JSON_VALUE(item, \'$.return_type\') AS return_type, cast(JSON_VALUE(item, \'$.quantity\') as int64) AS quantity, cast(JSON_VALUE(item, \'$.item_refund_amount\') as float64) AS item_refund_amount, JSON_VALUE(item, \'$.exchange_product_sku\') AS exchange_sku, 1 as return_flag FROM `maplemonk.pragma-webhook`, UNNEST(JSON_EXTRACT_ARRAY(replace(replace(replace(replace(REPLACE(line_items, \"\'\", \'\"\'),\'None\',\'null\'),\'True\',\'true\'),\'False\',\'false\'), \'\"null\"\', \'null\'))) AS item Qualify row_number() over(partition by order_name, JSON_VALUE(item, \'$.product_sku\') order by ingested_at desc ) = 1 UNION ALL Select upper(marketplace) marketplace, cast(return_complete_date as datetime) return_date, reference_code, return_sku, return_saleOrderItemCode, return_type as return_reason, \'refund\' as return_type, 1 as quantity, null as item_refund_amount, cast(null as string) exchange_sku, return_flag FROM freakins-wh.maplemonk.FREAKINS_UNICOMMERCE_RETURNS where return_flag = 1 and not(lower(marketplace) like any (\'%freakin%\',\'%shopify%\')) ;",
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
            