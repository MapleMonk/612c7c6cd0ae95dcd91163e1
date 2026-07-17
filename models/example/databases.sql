{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fresh_actual_allocation AS WITH raw AS ( SELECT bj.order_date, bj.final_store, mf.\"STORE CODE\"::VARCHAR AS branch_code, UPPER(bj.sku) AS sku, CASE WHEN UPPER(bj.sku) LIKE \'4C%\' THEN SPLIT_PART(bj.sku, \'-\', 1) || \'-\' || SPLIT_PART(bj.sku, \'-\', 2) || \'-\' || SPLIT_PART(bj.sku, \'-\', 3) WHEN POSITION(\'-\' IN bj.sku) > 0 THEN SPLIT_PART(bj.sku, \'-\', 1) || \'-\' || SPLIT_PART(bj.sku, \'-\', 2) ELSE bj.sku END AS sku_group, REVERSE(SUBSTRING(REVERSE(bj.sku), 1, POSITION(\'-\', REVERSE(bj.sku)) - 1)) AS raw_size, SUM(bj.qty) AS actual_qty FROM snitch_db.maplemonk.b2b_journey bj LEFT JOIN snitch_db.maplemonk.master_file mf ON TRIM(UPPER(bj.final_store)) = TRIM(UPPER(mf.\"STORE NAME\")) WHERE bj.order_name NOT LIKE \'QC-%\' AND UPPER(bj.type) LIKE \'%FRESH%\' AND UPPER(bj.sku) NOT LIKE \'CB%\' GROUP BY 1,2,3,4,5,6 ) SELECT order_date, final_store, branch_code, sku, sku_group, CASE WHEN sku LIKE \'SN%\' OR sku LIKE \'4MSFR%\' OR sku LIKE \'BP%\' THEN \'NA\' WHEN sku LIKE \'SH%\' THEN CASE raw_size WHEN \'39\' THEN \'XS\' WHEN \'40\' THEN \'S\' WHEN \'41\' THEN \'M\' WHEN \'42\' THEN \'L\' WHEN \'43\' THEN \'XL\' WHEN \'44\' THEN \'XXL\' WHEN \'45\' THEN \'3XL\' ELSE raw_size END ELSE CASE raw_size WHEN \'28\' THEN \'XS\' WHEN \'30\' THEN \'S\' WHEN \'32\' THEN \'M\' WHEN \'34\' THEN \'L\' WHEN \'36\' THEN \'XL\' WHEN \'38\' THEN \'XXL\' WHEN \'40\' THEN \'3XL\' WHEN \'42\' THEN \'4XL\' WHEN \'44\' THEN \'5XL\' WHEN \'46\' THEN \'6XL\' WHEN \'48\' THEN \'7XL\' WHEN \'50\' THEN \'8XL\' WHEN \'\' THEN \'NA\' WHEN NULL THEN \'NA\' ELSE raw_size END END AS size_mapped, actual_qty FROM raw WHERE branch_code IS NOT NULL;",
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
            