{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.pronk_inventory_grn_cost_fact_items AS SELECT i.sku, data_fetch_date, maplemonk.get_category(IFNULL(i.sku,\'\')) AS Category, maplemonk.get_sub_category(IFNULL(i.sku,\'\')) AS Product_Sub_Category, maplemonk.get_color(IFNULL(i.sku,\'\')) AS Colour, maplemonk.get_size(IFNULL(i.sku,\'\')) AS Size, maplemonk.get_dt_code(IFNULL(i.sku,\'\')) AS Product_DT_Code, maplemonk.get_gender(IFNULL(i.sku,\'\')) AS Gender, available_inventory, fi.avg_grn_cost AS grn_cost, CASE WHEN (Product_DT_Code IS NULL OR Product_DT_Code = \'NA\') THEN fi.avg_grn_cost ELSE (fi.avg_grn_cost + 60) END AS avg_grn_cost FROM maplemonk.pronk_inventory_fact_items i LEFT JOIN ( SELECT UPPER(maplemonk.get_sku_without_dt_code(IFNULL(TRIM(sku),\'\'))) AS sku, AVG(CAST(grn_cost AS FLOAT64)) AS avg_grn_cost FROM maplemonk.Easyecom_full_inventory_report GROUP BY 1 ) fi ON LOWER(fi.sku) = LOWER(maplemonk.get_sku_without_dt_code(i.sku));",
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
            