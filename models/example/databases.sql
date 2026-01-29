{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.unicommerce_live_inventory as select \"Category Name\"::string as \"Category Name\", Facility::string as Facility, \"Item SkuCode\"::string as \"Item SkuCode\", Inventory::INTEGER as Inventory, nullif(\"Inventory Blocked\",\'\')::INTEGER as \"Inventory Blocked\", nullif(\"Bad Inventory\",\'\')::INTEGER as \"Bad Inventory\", nullif(\"Inventory Not Synced\",\'\')::INTEGER as \"Inventory Not Synced\", nullif(\"Stock In Transfer\",\'\')::INTEGER as \"Stock In Transfer\", nullif(\"Not Found\",\'\')::INTEGER as \"Not Found\", nullif(\"Open Purchase\",\'\')::INTEGER as \"Open Purchase\", nullif(\"Open Sale\",\'\')::INTEGER as \"Open Sale\", nullif(\"Pending Inventory Assessment\",\'\')::INTEGER as \"Pending Inventory Assessment\", nullif(\"Putaway Pending\",\'\')::INTEGER as \"Putaway Pending\", nullif(\"Putback Pending\",\'\')::INTEGER as \"Putback Pending\", EAN::string as EAN, Enabled as Enabled, Color::string as Color, \"Cost Price\" as \"Cost Price\", ISBN::string as ISBN, Brand::String as Brand, \"Item Type Name\"::string as \"Item Type Name\", MRP as MRP, Size::string as Size, UPC::string as UPC, Updated::string as Updated, from snitch_db.maplemonk.unicommerce_get_inventory_snapshot_not_found where facility in (\'SAPL-WH1\',\'SAPL-WH2\',\'SAPL-NORTH-TAURU\')",
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
            