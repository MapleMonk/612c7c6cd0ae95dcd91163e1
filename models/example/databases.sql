{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.shelfwise_inventory_uc AS WITH shelfwise_inventory_cte AS ( SELECT FACILITY AS warehouse_name, \"Inventory Type\", SECTION, SHELF, CAST(quantity AS INTEGER) AS available_qty, CAST(\"Quantity Blocked\" AS INTEGER) AS blocked_qty, CAST(\"Quantity Damaged\" AS INTEGER) AS damaged_qty, CAST(\"Quantity Not Found\" AS INTEGER) AS notfound_qty, (CAST(quantity AS INTEGER) + CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) AS total_qty, CASE WHEN \"Inventory Sync\" = \'true\' AND \"Inventory Type\" LIKE \'GOOD%\' THEN CAST(quantity AS INTEGER) END AS sellable_qty, CASE WHEN \"Inventory Sync\" = \'false\' THEN (CAST(quantity AS INTEGER) + CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) WHEN \"Inventory Sync\" = \'true\' OR \"Inventory Type\" LIKE \'BAD%\' THEN (CAST(\"Quantity Blocked\" AS INTEGER) + CAST(\"Quantity Damaged\" AS INTEGER) + CAST(\"Quantity Not Found\" AS INTEGER)) END AS non_sellable_qty, \"Item Type SKU Code\", \"Sku Mixing\", \"Shelf On Hold\", \"Inventory Sync\", \"Inventory Allocation\" FROM snitch_db.maplemonk.uc_new_get_shelfwise_inventory WHERE FACILITY IN (\'SAPL_EMIZA\', \'SAPL-WH2\', \'SAPL-WH1\') ) SELECT * FROM shelfwise_inventory_cte; CREATE OR REPLACE TABLE snitch_db.maplemonk.inventory_bin_analysis AS SELECT a.enabled, a.zone, a.\"Shelf Code\", a.\"Sku Mixing\" as bin_mixing, a.\"Section Code\", a.\"Shelf on Hold\" as bin_onhold, a.\"Inventory Sync\" as bin_sync, a.\"Warehouse Name\" as facility, a.\"Shelf Type Code\", a.\"Shelf Status Code\", a.\"Inventory Allocation\" as bin_allocation, b.* FROM snitch_db.maplemonk.uc_new_get_shelf_report AS a LEFT JOIN snitch_db.maplemonk.shelfwise_inventory_uc AS b ON a.\"Shelf Code\" = b.SHELF WHERE a.\"Warehouse Name\" = \'SAPL-WH2\' AND (a.\"Section Code\" like \'F%\') and a.\"Shelf Status Code\" = \'ACTIVE\';",
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
            