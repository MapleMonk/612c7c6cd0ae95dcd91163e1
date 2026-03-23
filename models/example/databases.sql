{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ptal_INVENTORY_FACT_ITEMS_amazon_us as with amazon_fba_sales_7d as ( select product_id Asin, sku, sum(ifnull(quantity,0)) as sale_l7 from MAPLEMONK.P_TAL_DB_USA_AMAZON_FACT_ITEMS where cast(order_timestamp as date) between (select max(cast(order_timestamp as date)) -6 from MAPLEMONK.P_TAL_DB_USA_AMAZON_FACT_ITEMS) and (select max(cast(order_timestamp as date)) from MAPLEMONK.P_TAL_DB_AMAZON_FACT_ITEMS) group by 1,2 ), amazon_fba_sales_30d as ( select product_id Asin, sku, sum(ifnull(quantity,0)) as sale_l30 from MAPLEMONK.P_TAL_DB_USA_AMAZON_FACT_ITEMS where cast(order_timestamp as date) between (select max(cast(order_timestamp as date)) -29 from MAPLEMONK.P_TAL_DB_USA_AMAZON_FACT_ITEMS) and (select max(cast(order_timestamp as date)) from MAPLEMONK.P_TAL_DB_AMAZON_FACT_ITEMS) group by 1,2 ), amazon_fba_inventory as ( select cast(DATETIME(TIMESTAMP(_airbyte_normalized_at), \'Asia/Kolkata\') as date) date, asin, sku, cast(afn_warehouse_quantity as int) as inventory from MapleMonk.amazon_us_GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA qualify row_number() over (partition by asin,DATETIME(TIMESTAMP(_airbyte_normalized_at), \'Asia/Kolkata\') order by DATETIME(TIMESTAMP(_airbyte_normalized_at), \'Asia/Kolkata\') desc) = 1 ) select a.date, a.asin, coalesce(a.sku, b.sku, c.sku) sku, a.inventory, b.sale_l7, c.sale_l30 from amazon_fba_inventory a left join amazon_fba_sales_7d b on a.asin = b.asin and a.sku = b.sku left join amazon_fba_sales_30d c on a.asin = c.asin and a.sku = c.sku ;",
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
            