{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table snitch_db.maplemonk.Planned_putaway_identifier as WITH base AS ( SELECT UPPER(TRIM(cleaned_sku)) AS sku_group, CASE WHEN remarks_final ILIKE \'JAN%\' THEN 1 WHEN remarks_final ILIKE \'FEB%\' THEN 2 WHEN remarks_final ILIKE \'MAR%\' THEN 3 WHEN remarks_final ILIKE \'APR%\' THEN 4 WHEN remarks_final ILIKE \'MAY%\' THEN 5 WHEN remarks_final ILIKE \'JUN%\' THEN 6 WHEN remarks_final ILIKE \'JUL%\' THEN 7 WHEN remarks_final ILIKE \'AUG%\' THEN 8 WHEN remarks_final ILIKE \'SEP%\' THEN 9 WHEN remarks_final ILIKE \'OCT%\' THEN 10 WHEN remarks_final ILIKE \'NOV%\' THEN 11 WHEN remarks_final ILIKE \'DEC%\' THEN 12 ELSE NULL END AS planned_month FROM snitch_db.maplemonk.inwards_list_mom_v2_sku_list_with_month_remarks GROUP BY 1, 2 ), planned_skus AS ( SELECT DISTINCT sku_group FROM base WHERE sku_group IS NOT NULL AND planned_month IS NOT NULL ), inward AS ( SELECT UPPER( TRIM( REGEXP_REPLACE( \"Item Type skuCode\", \'-[0-9A-Za-z]{1,5}$\', \'\' ) ) ) AS sku_group, MONTH(putaway_completed_date) AS inward_month, SUM( putaway_completed_quantity ) AS inwarded_qty FROM snitch_db.maplemonk.putaway_tracking WHERE final_type ILIKE \'New inward\' AND is_trading ILIKE \'True\' AND MONTH(putaway_completed_date) > 4 AND YEAR(putaway_completed_date) = 2026 GROUP BY 1, 2 HAVING SUM(putaway_completed_quantity) > 100 ), final AS ( SELECT i.sku_group, i.inward_month, i.inwarded_qty, CASE WHEN p.sku_group IS NOT NULL THEN \'PLANNED\' ELSE \'UNPLANNED\' END AS status FROM inward i LEFT JOIN planned_skus p ON i.sku_group = p.sku_group ) SELECT sku_group, inward_month, CASE WHEN inward_month = 1 THEN \'JANUARY\' WHEN inward_month = 2 THEN \'FEBRUARY\' WHEN inward_month = 3 THEN \'MARCH\' WHEN inward_month = 4 THEN \'APRIL\' WHEN inward_month = 5 THEN \'MAY\' WHEN inward_month = 6 THEN \'JUNE\' WHEN inward_month = 7 THEN \'JULY\' WHEN inward_month = 8 THEN \'AUGUST\' WHEN inward_month = 9 THEN \'SEPTEMBER\' WHEN inward_month = 10 THEN \'OCTOBER\' WHEN inward_month = 11 THEN \'NOVEMBER\' WHEN inward_month = 12 THEN \'DECEMBER\' END AS inward_month_name, inwarded_qty, status FROM final ORDER BY inward_month, status, sku_group;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            