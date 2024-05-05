{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day as select * from snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day union select * from snitch_db.maplemonk.unicommerce_inventory_aging; create or replace table snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day as select \"SIZE\", \"BRAND\", \"COLOR\", \"CATEGORY\", \"FACILITY\", \"Item Code\", \"Tenant Id\", \"Expires On\", \"GRN Number\", \"Facility Id\", \"Vendor Code\", \"Vendor Name\", \"Item Type Name\", \"Item Created On\", \"Days in warehouse\", \"Item Type skuCode\", \"Custom Field Values\", \"Last Putaway Number\", \"Unit price with tax\", \"_AB_SOURCE_FILE_URL\", \"Last Putaway Created\", \"Unit price without tax\", \"_AB_ADDITIONAL_PROPERTIES\", \"_AB_SOURCE_FILE_LAST_MODIFIED\", \"_AIRBYTE_AB_ID\", \"_AIRBYTE_EMITTED_AT\", \"_AIRBYTE_NORMALIZED_AT\", \"_AIRBYTE_UNICOMMERCE_INVENTORY_AGING_HASHID\" from ( select *, dense_rank() over(partition by \"Item Type skuCode\",_airbyte_emitted_at::date order by _airbyte_emitted_at asc)rw1 from snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day ) where rw1 = 1",
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
                        