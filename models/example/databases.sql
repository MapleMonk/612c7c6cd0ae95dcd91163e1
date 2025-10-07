{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.DESIGN_FACT_ITEMS_L1_PJ AS SELECT d.*, CASE WHEN LOWER(d.sku_group) LIKE \'4mbg%\' THEN \'plus_size\' WHEN LOWER(d.style) LIKE \'%luxe%\' THEN \'luxe\' WHEN LOWER(d.style) LIKE \'%core%\' THEN \'core\' WHEN LOWER(d.category) IN (\'perfumes\',\'accessories\',\'bags\',\'belts\',\'sunglasses\',\'shoes\',\'slip-ons\') THEN \'long_tail\' ELSE \'snitch\' END AS l1_category FROM SNITCH_DB.MAPLEMONK.DESIGN_FACT_ITEMS d;",
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
            