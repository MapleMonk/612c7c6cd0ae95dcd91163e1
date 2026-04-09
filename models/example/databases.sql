{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \"Asia/Kolkata\"; create or replace table snitch_db.maplemonk.cogs_otb_v2_check2 as WITH default_inwards_cogs AS ( select TO_DATE(month, \'DD-MM-YYYY\') AS month, l_1 l1,cat,default_inwards_cogs from snitch_db.maplemonk.inwards_plan_default_sheet1 ) , meta_sku_map AS ( SELECT UPPER( IFF( UPPER(REPLACE(SKU_GROUP, \' \', \'\')) LIKE \'MP%\' OR UPPER(REPLACE(SKU_GROUP, \' \', \'\')) LIKE \'4C-%\', REGEXP_REPLACE(REPLACE(SKU_GROUP, \' \', \'\'), \'^([^-]+-[^-]+).*$\', \'\\1\'), REGEXP_REPLACE(REPLACE(SKU_GROUP, \' \', \'\'), \'-.*$\', \'\') ) ) AS sku_group_clean, COALESCE( MIN(CASE WHEN UPPER(TRIM(a.l1_category)) = \'LONG TAIL\' THEN a.l1_category END), MIN(CASE WHEN UPPER(TRIM(a.l1_category)) = \'PLUS\' THEN a.l1_category END), MIN(CASE WHEN UPPER(TRIM(a.l1_category)) = \'LUXE\' THEN a.l1_category END), MIN(CASE WHEN UPPER(TRIM(a.l1_category)) = \'SNITCH\' THEN a.l1_category END), MIN(a.l1_category) ) AS l1_category, COALESCE( MIN(CASE WHEN UPPER(TRIM(a.category)) = \'SHIRTS\' THEN a.category END), MIN(CASE WHEN UPPER(TRIM(a.category)) = \'TSHIRTS\' THEN a.category END), MIN(CASE WHEN UPPER(TRIM(a.category)) = \'JEANS\' THEN a.category END), MIN(CASE WHEN UPPER(TRIM(a.category)) = \'TROUSERS\' THEN a.category END), MIN(a.category) ) AS category, MAX(COALESCE(a.cogs, 0)) AS sku_cogs FROM snitch_db.maplemonk.meta_mapping_cogs_sku a GROUP BY 1 )select * from meta_sku_map;",
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
            