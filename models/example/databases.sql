{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.putaway_tracking AS SELECT \"Putaway Code\", CASE WHEN LOWER(\"Created By\") LIKE \'%store%\' THEN \'Store Return Inward\' WHEN LOWER(\"Created By\") LIKE \'%return%\' THEN \'WPR Inward\' WHEN LOWER(type) LIKE \'%grn%\' THEN \'New Inward\' WHEN LOWER(type) LIKE \'%reverse%\' OR LOWER(type) LIKE \'%return%\' THEN \'B2C Return Inward\' WHEN LOWER(type) LIKE \'%cancelled%\' OR LOWER(type) LIKE \'%picklist%\' THEN \'Putback Activity \' WHEN LOWER(type) LIKE \'%shelf%\' THEN \'Audit Activity\' WHEN LOWER(type) LIKE \'%gatepass%\' THEN \'Gatepass Activity\' ELSE \'BAU_PT\' END AS final_type, type AS putaway_type, created::timestamp AS putaway_created, updated::timestamp AS putaway_updated, \"Inventory Type\", CAST(\"Total Quantity\" AS NUMERIC) AS total_quantity, CAST(\"Putaway Completed Quantity\" AS NUMERIC) AS putaway_completed_quantity, CAST(\"Total Quantity\" - \"Putaway Completed Quantity\" AS NUMERIC) AS pending_qty, \"Putaway Status Code\", \"Putaway Item Status Code\", \"Bad inventory Reason\", \"Warehouse Name\", \"Putaway Item Id\", \"Item Type skuCode\", \"Putaway qc Comment\", \"Created By\", \"Shelf Code\", Category, EAN, NAME AS product_name, \"Batch Code\" FROM snitch_db.maplemonk.snitch_final_get_put_away_new WHERE updated::date >= \'2024-09-01\';",
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
            