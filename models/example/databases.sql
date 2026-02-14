{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `neshanka-wh.maplemonk.shopify_sales_inventory_fact_items` AS WITH sales_45_day AS ( SELECT SKU, SUM(quantity) as Sold_Quantity_45_Days FROM (select * from `neshanka-wh.maplemonk.NESHANKA_WH_sales_consolidated` where marketplace = \'WEBSITE\') WHERE DATE(order_Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) GROUP BY sku ), sales_agg AS ( SELECT SKU, product_name_final as product_name, product_category as product_type, vendor, order_date, SUM(quantity) as total_quantity, SUM(selling_price) as total_line_value FROM (select * from `neshanka-wh.maplemonk.NESHANKA_WH_sales_consolidated` where marketplace = \'WEBSITE\') o where ordeR_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) GROUP BY 1,2,3,4,5 ) SELECT sa.SKU, sa.product_name, sa.product_type, sa.vendor, sa.order_date, sa.total_quantity, sa.total_line_value, inv.Available_Inventory as WH_inv, sales_45.Sold_Quantity_45_Days, ROUND(inv.Available_Inventory / NULLIF(sales_45.Sold_Quantity_45_Days / 45, 0), 2) as DOC_45D, CURRENT_TIMESTAMP() as _load_timestamp FROM sales_agg sa LEFT JOIN ( select distinct sku, Available_Inventory, data_fetch_date from `neshanka-wh.maplemonk.neshanka_INVENTORY_FACT_ITEMS` ) inv ON LOWER(sa.SKU) = LOWER(REPLACE(inv.sku, \' \', \'\')) and order_date = data_fetch_date LEFT JOIN sales_45_day sales_45 ON LOWER(sa.SKU) = LOWER(sales_45.SKU);",
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
            