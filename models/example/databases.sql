{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.PREPACK_SO_CREATION AS WITH BASE AS ( SELECT ITEM_SKU_CODE, PO_CODE, UPDATED, \'NORTH\' AS WAREHOUSE FROM snitch_db.maplemonk.PREPACK_CODES_NORTH UNION ALL SELECT ITEM_SKU_CODE, PO_CODE, UPDATED, \'SOUTH\' FROM snitch_db.maplemonk.PREPACK_CODES_SOUTH ), ENRICHED AS ( SELECT ITEM_SKU_CODE, PO_CODE, DATEDIFF(\'DAY\', UPDATED, CURRENT_DATE()) AS AGEING, REGEXP_SUBSTR(ITEM_SKU_CODE, \'^PP[0-9]*-([A-Za-z0-9]+-[0-9]{2})\', 1, 1, \'e\', 1) AS SKU_GROUP, WAREHOUSE, REGEXP_REPLACE(ITEM_SKU_CODE, \'-[0-9]+$\', \'\') AS PREPACK_CODE FROM BASE ), GROUP_COUNTS AS ( SELECT SKU_GROUP, WAREHOUSE, COUNT(DISTINCT ITEM_SKU_CODE) AS PREPACK_COUNT FROM ENRICHED GROUP BY SKU_GROUP,WAREHOUSE ) SELECT E.ITEM_SKU_CODE, E.PO_CODE, E.AGEING, E.SKU_GROUP, E.WAREHOUSE, E.PREPACK_CODE, G.PREPACK_COUNT FROM ENRICHED E LEFT JOIN GROUP_COUNTS G ON E.SKU_GROUP = G.SKU_GROUP ORDER BY E.WAREHOUSE, E.ITEM_SKU_CODE;",
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
            