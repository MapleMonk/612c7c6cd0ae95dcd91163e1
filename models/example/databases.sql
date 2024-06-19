{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.store_replen_4 as WITH warehouse_rank AS ( SELECT SKU, facility, units_on_hand, ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY units_on_hand DESC) AS warehouse_rank FROM SNITCH_DB.MAPLEMONK.LIVE_INV_WAREHOUSE_offline_replen ), replen_allocation AS ( SELECT r.SKU_CODE, r.branch_code, r.branch_name, r.final_action, r.pareto, r.priority, r.REPLEN_UNITS, w.facility, w.units_on_hand, SUM(w.units_on_hand) OVER (PARTITION BY r.SKU_CODE ORDER BY w.warehouse_rank) - w.units_on_hand AS cumulative_inventory_before, SUM(w.units_on_hand) OVER (PARTITION BY r.SKU_CODE ORDER BY w.warehouse_rank) AS cumulative_inventory_after FROM snitch_db.maplemonk.store_replen_3 r JOIN warehouse_rank w ON r.SKU_CODE = w.SKU ) , final_allocation AS ( SELECT SKU_CODE, branch_code, branch_name, final_action, pareto, priority, facility, REPLEN_UNITS, units_on_hand, CASE WHEN cumulative_inventory_before < REPLEN_UNITS AND cumulative_inventory_after >= REPLEN_UNITS THEN REPLEN_UNITS - cumulative_inventory_before WHEN cumulative_inventory_after < REPLEN_UNITS THEN units_on_hand ELSE 0 END AS allocated_units FROM replen_allocation ) SELECT SKU_CODE, branch_code, branch_name, final_action, pareto, priority, facility, allocated_units FROM final_allocation WHERE allocated_units > 0 ORDER BY SKU_CODE, facility",
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
                        