{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table medmongers_db.maplemonk.medmongers_inventory_fact_items as WITH sales_aggregates AS ( SELECT upper(commonsku) AS sku, \'MEDMONGERS\' AS facility, SUM(CASE WHEN CAST(order_date AS DATE) = CURRENT_DATE THEN quantity ELSE 0 END) AS daily_quantity, SUM(CASE WHEN CAST(order_date AS DATE) >= CURRENT_DATE - 6 THEN quantity ELSE 0 END) AS sold_quantity_7_days, SUM(CASE WHEN CAST(order_date AS DATE) >= CURRENT_DATE - 13 THEN quantity ELSE 0 END) AS sold_quantity_14_days, SUM(CASE WHEN CAST(order_date AS DATE) >= CURRENT_DATE - 29 THEN quantity ELSE 0 END) AS sold_quantity_30_days, SUM(CASE WHEN CAST(order_date AS DATE) >= CURRENT_DATE - 44 THEN quantity ELSE 0 END) AS sold_quantity_45_days FROM medmongers_db.MAPLEMONK.medmongers_sales_consolidated WHERE CAST(order_date AS DATE) >= CURRENT_DATE - 45 GROUP BY 1, 2 ), current_inventory AS ( SELECT TO_DATE(CONVERT_TIMEZONE(\'Asia/Kolkata\', I._AIRBYTE_EMITTED_AT)) data_fetch_date, upper(replace(i.\"Item SkuCode\",\'`\',\'\')) sku, \'MEDMONGERS\' AS facility, sum(cast(inventory as INT)) as available_inventory, CASE WHEN ifnull(sum(cast(inventory as INT)),0) > 0 THEN 1 ELSE 0 END AS has_inventory_today, CASE WHEN ifnull(sum(cast(inventory as INT)),0) < 4 THEN 1 ELSE 0 END AS is_oos FROM medmongers_db.maplemonk.medmongers_uc_get_inventory_snapshot_export_full_refresh i GROUP BY 1,2,3 ORDER BY 1 DESC ) SELECT i.data_fetch_date, i.sku, i.facility, p.brand, p.product_name, p.category AS product_category, COALESCE(s.daily_quantity, 0) AS daily_quantity, i.has_inventory_today, i.available_inventory, COALESCE(s.sold_quantity_7_days, 0) AS sold_quantity_7_days, COALESCE(s.sold_quantity_14_days, 0) AS sold_quantity_14_days, COALESCE(s.sold_quantity_30_days, 0) AS sold_quantity_30_days, COALESCE(s.sold_quantity_45_days, 0) AS sold_quantity_45_days FROM current_inventory i LEFT JOIN sales_aggregates s ON i.sku = s.sku AND i.facility = s.facility LEFT JOIN ( SELECT upper(trim(SKU_CODE)) as sku, upper(product_name) as product_name, category, brand FROM MAPLEMONK.MEDMONGERS_FINAL_SKU_MASTER QUALIFY row_number() over (partition by upper(trim(SKU_CODE)) order by 1) = 1 ) p ON i.sku = p.sku;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            