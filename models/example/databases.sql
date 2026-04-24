{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prolicious-wh.maplemonk.prolicious_amazon_location_INVENTORY_FACT_ITEMS as with amazon_location_sales_7d as ( select asin, location, sum(abs(cast(customer_shipments as int))) as sale_l7 from Maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA where disposition = \'SELLABLE\' and date between (select max(cast(date as date)) -6 from maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA) and (select max(cast(date as date)) from maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA) group by 1,2 ), amazon_location_sales_30d as ( select asin, location, sum(abs(cast(customer_shipments as int))) as sale_l30 from Maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA where disposition = \'SELLABLE\' and date between (select max(cast(date as date)) -29 from maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA) and (select max(cast(date as date)) from maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA) group by 1,2 ), amazon_location_inventory as ( select date, asin, location, cast(ending_Warehouse_balance as int) as inventory from Maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA where disposition = \'SELLABLE\' and date = (select max(cast(date as date)) from maplemonk.Amazon_Inventory_GET_LEDGER_SUMMARY_VIEW_DATA) ) select a.date, a.asin, a.location, a.inventory, b.sale_l7, c.sale_l30 from amazon_location_inventory a left join amazon_location_sales_7d b on a.asin = b.asin and a.location = b.location left join amazon_location_sales_30d c on a.asin = c.asin and a.location = c.location ;",
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
            