{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.CATALOG_PHOTOSHOOT AS WITH catalog AS ( SELECT SKU_GROUP, STATUS, latest_inward_date, SUM(units_on_hand) AS units_on_hand FROM snitch_db.maplemonk.Inventory_planning_summary_snitch WHERE status IN (\'draft\', \'Not in Shopify\') GROUP BY SKU_GROUP, STATUS, latest_inward_date HAVING SUM(units_on_hand) > 0 ), PSLJ AS ( SELECT SKU, \"Factory_\'\", Sample_Received_Date, Photoshoot_Status, IMAGE_SELECTION FROM snitch_db.maplemonk.shoot_pslj ) SELECT c.SKU_GROUP, c.STATUS, c.latest_inward_date, pj.SKU, pj.\"Factory_\'\", pj.Sample_Received_Date, pj.Photoshoot_Status, pj.IMAGE_SELECTION, SUM(c.units_on_hand) AS total_units_on_hand, CASE WHEN pj.Photoshoot_Status IS NOT NULL THEN \'Catalog in Progress\' ELSE \'Photoshoot Not Done\' END AS \"Final Status\", CASE WHEN pj.SKU IS NULL THEN \'SKU_GROUP is not in PSLJ\' ELSE \'Present in PSLJ\' END AS \"PSLJ status\", CASE WHEN pj.Sample_Received_Date IS NULL THEN \'Sample Not Received\' ELSE \'Sample Received\' END AS \"Sample status\", CASE WHEN pj.Photoshoot_Status IS NULL THEN \'Photoshoot Not Done\' ELSE \'Photoshoot Done\' END AS \"Photoshoot Status\", CASE WHEN pj.IMAGE_SELECTION IS NULL THEN \'Image Selection Not Done\' ELSE \'Image Selection Done\' END AS \"Image Selection Status\" FROM catalog c LEFT JOIN PSLJ pj ON c.SKU_GROUP = pj.SKU GROUP BY c.SKU_GROUP, c.STATUS, c.latest_inward_date, pj.SKU, pj.\"Factory_\'\", pj.Sample_Received_Date, pj.Photoshoot_Status, pj.IMAGE_SELECTION;",
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
            