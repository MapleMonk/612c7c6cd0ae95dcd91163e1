{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.putaway_tracking AS SELECT a.\"Putaway Code\", CASE WHEN LOWER(a.\"Created By\") LIKE \'%store%\' THEN \'Store Return Inward\' WHEN LOWER(a.\"Created By\") LIKE \'%return%\' THEN \'WPR Inward\' WHEN LOWER(a.type) LIKE \'%grn%\' THEN \'New Inward\' WHEN LOWER(a.type) LIKE \'%reverse%\' OR LOWER(a.type) LIKE \'%return%\' THEN \'B2C Return Inward\' WHEN LOWER(a.type) LIKE \'%cancelled%\' OR LOWER(a.type) LIKE \'%picklist%\' THEN \'Putback Activity\' WHEN LOWER(a.type) LIKE \'%shelf%\' THEN \'Audit Activity\' WHEN LOWER(a.type) LIKE \'%gatepass%\' THEN \'Gatepass Activity\' ELSE \'BAU_PT\' END AS final_type, a.type AS putaway_type, a.created::timestamp AS putaway_created, a.updated::timestamp AS putaway_updated, a.\"Inventory Type\", CAST(a.\"Total Quantity\" AS NUMERIC) AS total_quantity, CAST(a.\"Putaway Completed Quantity\" AS NUMERIC) AS putaway_completed_quantity, CAST(a.\"Total Quantity\" - a.\"Putaway Completed Quantity\" AS NUMERIC) AS pending_qty, a.\"Putaway Status Code\", a.\"Putaway Item Status Code\", a.\"Bad inventory Reason\", a.\"Warehouse Name\", a.\"Putaway Item Id\", a.\"Item Type skuCode\", a.\"Putaway qc Comment\", a.\"Created By\", a.\"Shelf Code\", c.\"CATEGORY\" AS sku_category, c.sku_group, c.\"SKU_CLASS\" AS class, a.Category, a.EAN, a.NAME AS product_name, a.\"Batch Code\" FROM snitch_db.maplemonk.snitch_final_get_put_away_new AS a LEFT JOIN snitch_db.maplemonk.availability_master_v2 AS c ON LEFT(a.\"Item Type skuCode\", LENGTH(a.\"Item Type skuCode\") - CHARINDEX(\'-\', REVERSE(a.\"Item Type skuCode\"))) = c.\"SKU_GROUP\" WHERE a.updated::date >= \'2024-04-01\';",
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
            