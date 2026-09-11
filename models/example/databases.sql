{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.rts_reco as WITH final AS ( SELECT DISTINCT p.normalized_sku, p.status, q.sku AS online_sku, q.\"Logic PO\" FROM snitch_db.maplemonk.production_pipeline_data_v2 p LEFT JOIN snitch_db.maplemonk.online_goods_main q ON UPPER(TRIM(p.normalized_sku)) = UPPER(TRIM(q.sku)) WHERE p.status ILIKE \'RTS\' ) SELECT DISTINCT normalized_sku FROM final WHERE online_sku IS NULL AND normalized_sku NOT ILIKE \'MP-4SFS070%\' ORDER BY normalized_sku;",
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
            