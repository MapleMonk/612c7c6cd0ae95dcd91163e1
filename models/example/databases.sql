{{ config(
            materialized='table',
                post_hook={
                    "sql": "alter session set timezone = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.snitch_final_inventory_wh2 as select *, current_date() as table_date from ( select *,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',_airbyte_emitted_at :: DATETIME)::date as date ,row_number() over( partition by \"Item SkuCode\",date,facility order by _airbyte_emitted_at) rw from ( select _AIRBYTE_DATA:\"Bad Inventory\" as \"Bad Inventory\", _AIRBYTE_DATA:Brand::String as Brand, _AIRBYTE_DATA:\"Category Name\"::string as \"Category Name\", _AIRBYTE_DATA:Color::string as Color, _AIRBYTE_DATA:\"Cost Price\" as \"Cost Price\", _AIRBYTE_DATA:EAN::string as EAN, _AIRBYTE_DATA:Enabled as Enabled, _AIRBYTE_DATA:Facility::string as Facility, _AIRBYTE_DATA:\"GRN Price With Tax\" as \"GRN Price With Tax\", _AIRBYTE_DATA:\"GRN Price Without Tax\" as \"GRN Price Without Tax\", _AIRBYTE_DATA:ISBN::string as ISBN, _AIRBYTE_DATA:Inventory as Inventory, _AIRBYTE_DATA:\"Inventory Blocked\" as \"Inventory Blocked\", _AIRBYTE_DATA:\"Inventory Not Synced\" as \"Inventory Not Synced\", _AIRBYTE_DATA:\"Item SkuCode\"::string as \"Item SkuCode\", _AIRBYTE_DATA:\"Item Type Name\"::string as \"Item Type Name\", _AIRBYTE_DATA:MRP as MRP, _AIRBYTE_DATA:\"Not Found\"::string as \"Not Found\", _AIRBYTE_DATA:\"Open Purchase\" as \"Open Purchase\", _AIRBYTE_DATA:\"Open Sale\" as \"Open Sale\", _AIRBYTE_DATA:\"Pending Inventory Assessment\" as \"Pending Inventory Assessment\", _AIRBYTE_DATA:\"Putaway Pending\" as \"Putaway Pending\", _AIRBYTE_DATA:\"Putback Pending\" as \"Putback Pending\", _AIRBYTE_DATA:Size::string as Size, _AIRBYTE_DATA:\"Stock In Transfer\" as \"Stock In Transfer\", _AIRBYTE_DATA:UPC::string as UPC, _AIRBYTE_DATA:Updated::string as Updated, _airbyte_emitted_at from snitch_db.maplemonk._airbyte_raw_snitch_inventory_wh2 ) ) where rw = 1",
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
            