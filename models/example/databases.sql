{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.inventory_bin_analysis AS WITH shelfwise_inventory_cte AS ( SELECT FACILITY AS warehouse_name, \"Inventory Type\", SECTION, SHELF, CAST(quantity AS INTEGER) AS available_qty, CAST(\"Quantity Blocked\" AS INTEGER) AS blocked_qty, CAST(\"Quantity Damaged\" AS INTEGER) AS damaged_qty, CAST(\"Quantity Not Found\" AS INTEGER) AS notfound_qty, (CAST(quantity AS INTEGER) + CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) AS total_qty, CASE WHEN \"Inventory Sync\" = \'true\' AND \"Inventory Type\" LIKE \'GOOD%\' THEN CAST(quantity AS INTEGER) ELSE NULL END AS sellable_qty, CASE WHEN \"Inventory Sync\" = \'false\' THEN total_qty WHEN \"Inventory Type\" like \'%BAD%\' THEN total_qty WHEN \"Inventory Sync\" = \'true\' OR \"Inventory Type\" LIKE \'BAD%\' THEN (CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) ELSE NULL END AS non_sellable_qty, \"Item Type SKU Code\" FROM snitch_db.maplemonk.uc_new_get_shelfwise_inventory WHERE FACILITY IN (\'SAPL_EMIZA\', \'SAPL-WH2\', \'SAPL-WH1\') ) SELECT a.enabled, a.zone, b.\"Inventory Type\", a.\"Shelf Code\", a.\"Sku Mixing\" AS bin_mixing, a.\"Section Code\", a.\"Shelf on Hold\" AS bin_onhold, a.\"Inventory Sync\" AS bin_sync, a.\"Warehouse Name\" AS facility, a.\"Shelf Type Code\", a.\"Shelf Status Code\", a.\"Inventory Allocation\" AS bin_allocation, b.available_qty, b.blocked_qty, b.damaged_qty, b.notfound_qty, b.total_qty, b.\"Item Type SKU Code\" as sku, b.sellable_qty, b.non_sellable_qty, c.sku_group, c.\"SKU_CLASS\", c.\"CATEGORY\" AS sku_category, CASE WHEN LEFT(a.\"Shelf Code\", 1) = \'F\' THEN \'NEW_BIN\' ELSE \'OLD_BIN\' END AS bin_category, REGEXP_SUBSTR(SPLIT_PART(a.\"Shelf Code\", \'-\', 1), \'[A-Z]+$\') AS ROW_name, SPLIT_PART(a.\"Shelf Code\", \'-\', 2) AS RACK_name, SPLIT_PART(a.\"Shelf Code\", \'-\', 3) AS SHELF_name, SPLIT_PART(a.\"Shelf Code\", \'-\', 4) AS BIN_name FROM snitch_db.maplemonk.uc_new_get_shelf_report AS a LEFT JOIN shelfwise_inventory_cte AS b ON a.\"Shelf Code\" = b.SHELF LEFT JOIN snitch_db.maplemonk.availability_master_v2 AS c ON LEFT(b.\"Item Type SKU Code\", LENGTH(b.\"Item Type SKU Code\") - CHARINDEX(\'-\', REVERSE(b.\"Item Type SKU Code\"))) = c.\"SKU_GROUP\" WHERE FACILITY IN (\'SAPL_EMIZA\', \'SAPL-WH2\', \'SAPL-WH1\')",
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
            