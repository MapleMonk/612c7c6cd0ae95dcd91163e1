{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.putaway_tracking AS ( SELECT REGEXP_REPLACE(\"_AB_SOURCE_FILE_URL\", \'^testingPhase/maplemonk/Putaway_report/PutawayReport_(SAPL-[A-Z]+).csv$\', \'\\1\') AS Warehouse, CASE WHEN \"Inventory Type\" = \'GOOD_INVENTORY\' THEN \'Good Inventory\' WHEN \"Inventory Type\" = \'BAD_INVENTORY\' THEN \'Bad Inventory\' ELSE \'Unknown\' END AS Inventory_Type, CASE WHEN \"TYPE\" = \'PUTAWAY_REVERSE_PICKUP_ITEM\' THEN \'RTO\' WHEN \"TYPE\" = \'PUTAWAY_RECEIVED_RETURNS\' THEN \'DTO\' WHEN \"TYPE\" = \'PUTAWAY_PICKLIST_ITEM\' THEN \'PICKLIST ITEM\' WHEN \"TYPE\" = \'PUTAWAY_CANCELLED_ITEM\' THEN \'CANCELLED\' WHEN \"TYPE\" = \'PUTAWAY_GRN_ITEM\' THEN \'GRN\' WHEN \"TYPE\" = \'PUTAWAY_SHELF_TRANSFER\' THEN \'SHELF TRANSFER\' ELSE \'Unknown\' END AS TYPE, \"Status Code\" AS Current_Status, DATE(\"CREATED\") AS CREATED_DATE, COUNT(*) AS total FROM snitch_db.maplemonk.unicommerce_putaway_report WHERE \"TYPE\" IN (\'PUTAWAY_REVERSE_PICKUP_ITEM\', \'PUTAWAY_RECEIVED_RETURNS\', \'PUTAWAY_PICKLIST_ITEM\', \'PUTAWAY_CANCELLED_ITEM\', \'PUTAWAY_GRN_ITEM\', \'PUTAWAY_SHELF_TRANSFER\') GROUP BY REGEXP_REPLACE(\"_AB_SOURCE_FILE_URL\", \'^testingPhase/maplemonk/Putaway_report/PutawayReport_(SAPL-[A-Z]+).csv$\', \'\\1\'), CASE WHEN \"Inventory Type\" = \'GOOD_INVENTORY\' THEN \'Good Inventory\' WHEN \"Inventory Type\" = \'BAD_INVENTORY\' THEN \'Bad Inventory\' ELSE \'Unknown\' END, CASE WHEN \"TYPE\" = \'PUTAWAY_REVERSE_PICKUP_ITEM\' THEN \'RTO\' WHEN \"TYPE\" = \'PUTAWAY_RECEIVED_RETURNS\' THEN \'DTO\' WHEN \"TYPE\" = \'PUTAWAY_PICKLIST_ITEM\' THEN \'PICKLIST ITEM\' WHEN \"TYPE\" = \'PUTAWAY_CANCELLED_ITEM\' THEN \'CANCELLED\' WHEN \"TYPE\" = \'PUTAWAY_GRN_ITEM\' THEN \'GRN\' WHEN \"TYPE\" = \'PUTAWAY_SHELF_TRANSFER\' THEN \'SHELF TRANSFER\' ELSE \'Unknown\' END, DATE(\"CREATED\"), \"Status Code\" );",
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
                        