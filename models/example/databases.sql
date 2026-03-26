{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.D2P_logic_EDD as ( SELECT p.SKU_GROUP, p.PD_ACCEPTANCE_STATUS, MIN(q.EDD) AS EDD FROM snitch_db.maplemonk.D2P_TOOL_TRANSFORMED_DATES p LEFT JOIN snitch_db.maplemonk.logic_purchase_order q ON UPPER(TRIM(p.SKU_GROUP)) = UPPER(TRIM(q.sku_group)) WHERE p.PD_ACCEPTANCE_STATUS ILIKE \'Direct FOB\' GROUP BY p.SKU_GROUP, p.PD_ACCEPTANCE_STATUS )",
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
            