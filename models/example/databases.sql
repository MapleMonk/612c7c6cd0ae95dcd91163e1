{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_sku_Level_cogs` AS WITH Category_Cogs AS ( SELECT PARSE_DATE(\'%d-%b-%y\', START_DATE) AS START_DATE, PARSE_DATE(\'%d-%b-%y\', END_DATE) AS END_DATE, TRIM(LOWER(category_code)) AS category_code, CAST(REPLACE(cogs, \',\', \'\') AS FLOAT64) AS cogs FROM MapleMonk.zouk_db_sku_mrp_cogs QUALIFY ROW_NUMBER() OVER ( PARTITION BY START_DATE, END_DATE, LOWER(category_code) ORDER BY 1 ) = 1 ), Sku_Map AS ( WITH deduped_pm AS ( SELECT * FROM `MapleMonk.Zouk_uc_get_product_master` QUALIFY ROW_NUMBER() OVER ( PARTITION BY UPPER(TRIM(Product_Code)), UPPER(TRIM(COALESCE(Component_Product_Code, Product_Code))) ORDER BY DATE(updated) DESC ) = 1 ) SELECT UPPER(TRIM(pm.product_code)) AS parent_sku, UPPER(COALESCE(NULLIF(TRIM(pm.component_product_code), \'\'), pm.product_code)) AS child_sku, CASE WHEN LOWER(TRIM(pm.type)) = \'bundle\' THEN LOWER(TRIM(COALESCE(child_pm.category_code, pm.category_code))) ELSE LOWER(TRIM(pm.category_code)) END AS final_category_code, CASE WHEN LOWER(TRIM(pm.type)) = \'bundle\' THEN true ELSE false END AS is_bundle FROM deduped_pm pm LEFT JOIN deduped_pm child_pm ON UPPER(TRIM(pm.component_product_code)) = UPPER(TRIM(child_pm.product_code)) AND LOWER(child_pm.type) = \'simple\' ), test AS ( SELECT c.START_DATE, c.END_DATE, s.parent_sku, s.child_sku, s.is_bundle, UPPER(s.final_category_code) AS category_code, c.cogs AS child_cogs FROM Sku_Map s LEFT JOIN Category_Cogs c ON LOWER(s.final_category_code) = LOWER(c.category_code) ) SELECT START_DATE, END_DATE, parent_sku, child_sku, is_bundle, SUM(child_cogs) OVER (PARTITION BY parent_sku, START_DATE, END_DATE) AS parent_cogs, child_cogs FROM test ; CREATE OR REPLACE TABLE `MapleMonk.Zouk_sku_Level_cogs_parent` AS SELECT DISTINCT START_DATE, END_DATE, parent_sku, parent_cogs AS cogs, false AS is_bundle FROM `MapleMonk.Zouk_sku_Level_cogs` QUALIFY ROW_NUMBER() OVER ( PARTITION BY parent_sku, START_DATE, END_DATE ORDER BY 1 ) = 1 and start_date is not null",
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
            