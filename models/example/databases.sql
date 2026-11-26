{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table `MapleMonk.Zouk_pandl_home` AS WITH logistics AS ( SELECT UPPER(TRIM(category_code)) AS category_code_key, ANY_VALUE(category_name) AS category_name, MAX(SAFE_CAST(REPLACE(TRIM(cost), \',\', \'\') AS FLOAT64)) AS logistics_cost FROM `MapleMonk.Zouk_Logistics_Home` GROUP BY 1 ) SELECT p.*, l.logistics_cost AS logistics_cost_per_unit, l.logistics_cost * p.quantity AS forward_logistics_cost FROM `MapleMonk.zouk_secondary_pandl` p LEFT JOIN logistics l ON UPPER(TRIM(p.category_code)) = l.category_code_key;",
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
            