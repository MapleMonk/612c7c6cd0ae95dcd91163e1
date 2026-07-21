{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fresh_actual_allocation AS SELECT * FROM ( SELECT bj.order_date, bj.final_store, mf.\"STORE CODE\"::VARCHAR AS branch_code, CASE WHEN UPPER(bj.sku) LIKE \'4C-%\' THEN SPLIT_PART(bj.sku, \'-\', 1) || \'-\' || SPLIT_PART(bj.sku, \'-\', 2) || \'-\' || SPLIT_PART(bj.sku, \'-\', 3) WHEN UPPER(bj.sku) LIKE \'MP-%\' THEN SPLIT_PART(bj.sku, \'-\', 1) || \'-\' || SPLIT_PART(bj.sku, \'-\', 2) || \'-\' || SPLIT_PART(bj.sku, \'-\', 3) WHEN POSITION(\'-\' IN bj.sku) > 0 THEN SPLIT_PART(bj.sku, \'-\', 1) || \'-\' || SPLIT_PART(bj.sku, \'-\', 2) ELSE bj.sku END AS sku_group, SUM(bj.qty) AS actual_qty FROM snitch_db.maplemonk.b2b_journey bj LEFT JOIN snitch_db.maplemonk.master_file mf ON TRIM(UPPER(bj.final_store)) = TRIM(UPPER(mf.\"STORE NAME\")) WHERE bj.order_name NOT LIKE \'QC-%\' AND UPPER(bj.type) LIKE \'%FRESH%\' AND UPPER(bj.sku) NOT LIKE \'CB%\' GROUP BY 1,2,3,4 ) WHERE branch_code IS NOT NULL;",
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
            