{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE snitch_db.maplemonk.sellable_inv_across_facilities AS ( SELECT DATE::date AS date, \'STORE\' as TYPE, BRANCH_NAME, LOGICUSERCODE as sku, CASE WHEN POSITION(\'-\' IN \"SKU\") > 0 AND LEFT(\"SKU\", 2) = \'SH\' AND LENGTH(\"SKU\") < 10 THEN UPPER(SPLIT_PART(\"SKU\", \'-\', 1)) WHEN POSITION(\'-\' IN \"SKU\") > 0 THEN UPPER(SPLIT_PART(\"SKU\", \'-\', 1)) || \'-\' || UPPER(SPLIT_PART(\"SKU\", \'-\', 2)) ELSE UPPER(\"SKU\") END AS sku_group, sum(STOCK_QTY) as inventory FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand WHERE DATE::date = current_date() and godown_name in (\'POS\',\'FRANCHISE\') group by 1,2,3,4,5 ) UNION ALL ( SELECT date, \'WAREHOUSE\' as TYPE, CASE WHEN facility = \'SAPL_EMIZA\' THEN \'SNITCH - WH - EMIZA\' WHEN facility = \'SAPL-WH2\' THEN \'SNITCH - WH - YELHANKA 2\' WHEN facility = \'SAPL-WH1\' THEN \'SNITCH - WH - YELHANKA\' ELSE \'NA\' END AS branch_name, \"Item SkuCode\" AS sku, CASE WHEN POSITION(\'-\' IN \"SKU\") > 0 AND LEFT(\"SKU\", 2) = \'SH\' AND LENGTH(\"SKU\") < 10 THEN UPPER(SPLIT_PART(\"SKU\", \'-\', 1)) WHEN POSITION(\'-\' IN \"SKU\") > 0 THEN UPPER(SPLIT_PART(\"SKU\", \'-\', 1)) || \'-\' || UPPER(SPLIT_PART(\"SKU\", \'-\', 2)) ELSE UPPER(\"SKU\") END AS sku_group, SUM(CAST(COALESCE(NULLIF(inventory, \'\'), 0) AS INTEGER)) AS inventory FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE facility IN (\'SAPL-WH1\', \'SAPL-WH2\', \'SAPL_EMIZA\') AND date = CURRENT_DATE group by 1,2,3,4,5 )",
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
            