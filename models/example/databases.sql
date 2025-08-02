{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT SUBSTRING(a.sku, 2) AS sku_group, CASE WHEN DATE_TRUNC(\'month\',TO_DATE(REPLACE(a.expected_delivery_date, \'-\', \'/\'),\'DD/MM/YYYY\')) = \'0025-09-01\' THEN \'2025-09-01\' ELSE DATE_TRUNC(\'month\',TO_DATE(REPLACE(a.expected_delivery_date, \'-\', \'/\'),\'DD/MM/YYYY\')) END AS month, SUM(a.proj_qty) AS supply_qty FROM snitch_db.maplemonk.prod_demand_noos a GROUP BY 1,2 ; noos as ( select upper(trim(SUBSTRING(sku, 2))) AS sku_group, CASE WHEN DATE_TRUNC(\'month\',TO_DATE(REPLACE(expected_delivery_date, \'-\', \'/\'),\'DD/MM/YYYY\')) = \'0025-09-01\' THEN \'2025-09-01\' ELSE TO_DATE(REPLACE(expected_delivery_date, \'-\', \'/\'),\'DD/MM/YYYY\') END AS expected_delivery_date from snitch_db.maplemonk.prod_demand_noos where status = \'Active\' ), repeats as ( select upper(trim(SUBSTRING(sku, 2))) AS sku_group, TO_DATE(REPLACE(\"EXPECTED_DELIVERY_DATE \", \'-\', \'/\'),\'DD/MM/YYYY\') AS expected_delivery_date FROM snitch_db.maplemonk.prod_demand_repeat where sku_status = \'Active\' ), fob as ( select upper(trim(sku)) as sku_group, TO_DATE(REPLACE(EXPECTED_DELIVERY_DATE, \'-\', \'/\'),\'DD/MM/YYYY\') AS expected_delivery_date from snitch_db.maplemonk.prod_demand_fob where status = \'Active\' ), conversion as ( select upper(trim(sku)) as sku_group, TO_DATE(REPLACE(EXPECTED_DELIVERY_DATE, \'-\', \'/\'),\'DD/MM/YYYY\') AS expected_delivery_date FROM snitch_db.maplemonk.prod_demand_conversion where sku_status_ = \'Active\' ), final_check_in_production as ( select * from noos union all select * from repeats union all select * from fob union all select * from conversion ), already_in_pipeline as ( select *, \'yes\' as final_check_in_production from final_check qualify row_number() over (partition by sku_group order by expected_delivery_date asc) = 1 ),",
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
            