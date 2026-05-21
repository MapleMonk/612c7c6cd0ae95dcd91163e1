{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table goodtribe-wh.maplemonk.pandl_summary as with sales as ( select reference_Code, ordeR_Date, invoice_Date, sku, SALEORDERITEMCODE, selling_price, quantity, oms_order_status, shipping_price, discount, rto_flag_new, tax, cogs, spend from goodtribe-wh.maplemonk.goodtribe_wh_sales_consolidated ), returns as ( select ordeR_name, cast(left(return_requested_at,10) as date) return_date, concat(returned_sku,\'-\',returned_product_variant) sku, order_item_id, cast(returned_product_price as float64) returned_producT_price, returned_quantity, shipping_status, total_discount, cast(tax as float64) tax, cogs from goodtribe-wh.maplemonk.goodtribe_RETURNSPRIME_CONSOLIDATED where return_request_type in (\'return\',\'exchange\') ) select reference_Code, ordeR_Date, invoice_Date, sku, SALEORDERITEMCODE, selling_price, quantity, oms_order_status, shipping_price, discount, rto_flag_new, tax, cogs, spend, null as returned_ordeR_name, null as returned_date, null as returned_sku, null as return_order_item_id, null as returned_product_price, null as returned_quantity, null as returned_shipping_status, null as returned_total_discount, null as returned_total_tax, null as returned_cogs from sales union all select null as reference_Code, return_date as ordeR_Date, return_date as invoice_Date, null as sku, null as SALEORDERITEMCODE, null as selling_price, null as quantity, null as oms_order_status, null as shipping_price, null as discount, null as rto_flag_new, null as tax, null as cogs, null as spend, ordeR_name, return_date, sku, order_item_id, returned_product_price, returned_quantity, shipping_status, total_discount, tax, cogs from returns ;",
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
            