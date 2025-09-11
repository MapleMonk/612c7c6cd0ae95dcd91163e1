{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.inventory_bin_analysis AS WITH shelfwise_inventory_cte AS ( SELECT FACILITY AS warehouse_name, \"Inventory Type\", SECTION, SHELF, CAST(quantity AS INTEGER) AS available_qty, CAST(\"Quantity Blocked\" AS INTEGER) AS blocked_qty, CAST(\"Quantity Damaged\" AS INTEGER) AS damaged_qty, CAST(\"Quantity Not Found\" AS INTEGER) AS notfound_qty, (CAST(quantity AS INTEGER) + CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) AS total_qty, CASE WHEN \"Inventory Sync\" = \'true\' AND \"Inventory Type\" LIKE \'GOOD%\' THEN CAST(quantity AS INTEGER) ELSE NULL END AS sellable_qty, CASE WHEN \"Inventory Sync\" = \'false\' THEN total_qty WHEN \"Inventory Type\" like \'%BAD%\' THEN total_qty WHEN \"Inventory Sync\" = \'true\' OR \"Inventory Type\" LIKE \'BAD%\' THEN (CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) ELSE NULL END AS non_sellable_qty, \"Item Type SKU Code\" FROM snitch_db.maplemonk.uc_new_get_shelfwise_inventory WHERE FACILITY IN (\'SAPL_EMIZA\', \'SAPL-WH2\', \'SAPL-WH1\',\'SAPL-NORTH-TAURU\') ), shelf as ( select enabled, zone, \"Shelf Code\", \"Sku Mixing\" AS bin_mixing, \"Section Code\", \"Shelf on Hold\" AS bin_onhold, \"Inventory Sync\" AS bin_sync, \"Warehouse Name\" AS facility, \"Shelf Type Code\", \"Shelf Status Code\", \"Inventory Allocation\" AS bin_allocation from snitch_db.maplemonk.uc_new_get_shelf_report ) SELECT a.*, b.warehouse_name, b.\"Inventory Type\", b.available_qty, b.blocked_qty, b.damaged_qty, b.notfound_qty, b.total_qty, b.\"Item Type SKU Code\" as sku, b.sellable_qty, b.non_sellable_qty, c.skugroup, c.\"SKU_CLASS\", c.category_code AS sku_category, CASE WHEN LEFT(a.\"Shelf Code\", 1) = \'F\' THEN \'NEW_BIN\' ELSE \'OLD_BIN\' END AS bin_category, REGEXP_SUBSTR(SPLIT_PART(a.\"Shelf Code\", \'-\', 1), \'[A-Z]+$\') AS ROW_name, SPLIT_PART(a.\"Shelf Code\", \'-\', 2) AS RACK_name, SPLIT_PART(a.\"Shelf Code\", \'-\', 3) AS SHELF_name, SPLIT_PART(a.\"Shelf Code\", \'-\', 4) AS BIN_name FROM shelf AS a LEFT JOIN shelfwise_inventory_cte AS b ON CONCAT(a.facility,a.\"Shelf Code\") = CONCAT(b.warehouse_name,b.SHELF) LEFT JOIN snitch_db.maplemonk.uc_final_item_master AS c ON UPPER(TRIM(\"Item Type SKU Code\")) = UPPER(TRIM(c.sku)) WHERE FACILITY IN (\'SAPL_EMIZA\', \'SAPL-WH2\', \'SAPL-WH1\',\'SAPL-NORTH-TAURU\')",
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
            