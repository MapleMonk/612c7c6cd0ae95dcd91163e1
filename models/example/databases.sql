{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.noos_tracking_eoq AS WITH putaway_cte AS ( SELECT UPPER(TRIM(REVERSE(SUBSTRING(REVERSE(\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(\"Item Type skuCode\")) + 1, LEN(\"Item Type skuCode\"))))) AS sku_group, DATE_TRUNC(\'MONTH\', \"PUTAWAY_UPDATED\"::date) AS date, SUM(\"PUTAWAY_COMPLETED_QUANTITY\")::INT AS actual_quantity FROM snitch_db.maplemonk.putaway_tracking WHERE \"PUTAWAY_UPDATED\" >= TO_TIMESTAMP(\'2025-06-01T00:00:00.000000\') AND LOWER(FINAL_TYPE) = \'new inward\' GROUP BY 1, 2 ), online_actual AS ( SELECT DISTINCT sku_group, category FROM snitch_db.maplemonk.noos_visibility WHERE nos_channel = \'Online\' ), offline_actual AS ( SELECT DISTINCT sku_group, category FROM snitch_db.maplemonk.noos_visibility WHERE nos_channel = \'Offline\' ), marketplace_actual AS ( SELECT DISTINCT MARKETPLACE_SKU AS sku_group, CATEGORY AS category FROM snitch_db.maplemonk.gs_jas_noos___mp WHERE DEPARTMENT LIKE \'MP%\' ), actuals AS ( SELECT o.sku_group, o.category, p.date, COALESCE(p.actual_quantity, 0) AS actual_quantity, 0 AS projected_quantity, \'Online\' AS channel FROM online_actual o LEFT JOIN putaway_cte p ON o.sku_group = p.sku_group UNION ALL SELECT f.sku_group, f.category, p.date, COALESCE(p.actual_quantity, 0) AS actual_quantity, 0 AS projected_quantity, \'Offline\' AS channel FROM offline_actual f LEFT JOIN putaway_cte p ON f.sku_group = p.sku_group UNION ALL SELECT m.sku_group, m.category, p.date, COALESCE(p.actual_quantity, 0) AS actual_quantity, 0 AS projected_quantity, \'Marketplace\' AS channel FROM marketplace_actual m LEFT JOIN putaway_cte p ON m.sku_group = p.sku_group ), projections AS ( SELECT SKU AS sku_group, CATEGORY AS category, TO_DATE(REPLACE(REVISED_DELIVERY_DATE, \'/\', \'-\'), \'DD-MM-YYYY\') AS date, 0 AS actual_quantity, PROJ_QTY::INT AS projected_quantity, CASE WHEN DEPARTMENT = \'NOOS Online\' THEN \'Online\' WHEN DEPARTMENT = \'NOOS Offline\' THEN \'Offline\' WHEN DEPARTMENT LIKE \'MP%\' THEN \'Marketplace\' END AS channel FROM snitch_db.maplemonk.gs_jas_noos___mp ) SELECT * FROM actuals UNION ALL SELECT * FROM projections;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            