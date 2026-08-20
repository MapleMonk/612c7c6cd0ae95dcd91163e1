{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.offline_master_1205 AS WITH category_mix_map AS ( SELECT branch_code, marketplace_mapped, category, MAX(cat_mix) AS cat_mix FROM snitch_db.maplemonk.offline_metafield_ideal_mix GROUP BY branch_code, marketplace_mapped, category ) SELECT om.*, cm.cat_mix FROM snitch_db.maplemonk.offline_master om LEFT JOIN category_mix_map cm ON om.branch_code = cm.branch_code AND UPPER(TRIM(om.marketplace_mapped)) = UPPER(TRIM(cm.marketplace_mapped)) AND UPPER( TRIM( CASE WHEN UPPER(TRIM(om.new_category)) = \'CHINOS\' THEN \'TROUSERS\' ELSE om.new_category END ) ) = UPPER(TRIM(cm.category));",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            