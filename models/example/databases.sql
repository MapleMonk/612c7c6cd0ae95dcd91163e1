{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.store_replen_5_May2025 as WITH SKU_SUMMARY AS ( SELECT SKU_CODE, SUM(TOTAL_REPLEN_REQ) AS CUML_REPLEN_REQ, SUM(ALLOCATED_UNITS) AS CUML_ALLO FROM snitch_db.maplemonk.store_replen_4_May2025 GROUP BY SKU_CODE ), INTERIM_ALLO AS ( SELECT t.SKU_CODE, t.BRANCH_CODE, t.FINAL_ACTION, t.PARETO, t.PRIORITY, t.FACILITY, t.REPLEN_ORDER, t.TOTAL_REPLEN_REQ, t.ALLOCATED_UNITS, s.CUML_REPLEN_REQ, s.CUML_ALLO, CASE WHEN s.CUML_ALLO >= s.CUML_REPLEN_REQ THEN t.ALLOCATED_UNITS ELSE CEIL(s.CUML_ALLO * t.TOTAL_REPLEN_REQ / s.CUML_REPLEN_REQ) END AS INTERIM_ALLO FROM snitch_db.maplemonk.store_replen_4_May2025 t INNER JOIN SKU_SUMMARY s ON t.SKU_CODE = s.SKU_CODE ), SOCIALISTIC_ALLO AS ( SELECT i.*, CASE WHEN SUM(INTERIM_ALLO) OVER (PARTITION BY SKU_CODE ORDER BY REPLEN_ORDER ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= CUML_ALLO THEN INTERIM_ALLO ELSE 0 END AS SOCIALISTIC FROM INTERIM_ALLO i ) SELECT * FROM SOCIALISTIC_ALLO; create or replace table snitch_db.maplemonk.store_replen_6_May2025 as with Agg_JIT AS ( SELECT \"ITEM CODE\" AS SKU_CODE, BRANCH_CODE, SUM(QTY) AS total_jit_qty FROM SNITCH_DB.MAPLEMONK.JIT_OFFLINE_GOODS GROUP BY \"ITEM CODE\", BRANCH_CODE ), Agg_L AS ( SELECT ADDLITEMCODE AS SKU_CODE, BRANCH_CODE, SUM(STOCK_QTY) AS total_stock_qty FROM SNITCH_DB.MAPLEMONK.LOGICERP23_24_GET_STOCK_IN_HAND WHERE DATE = CURRENT_DATE GROUP BY ADDLITEMCODE, BRANCH_CODE ) SELECT S5.* FROM SNITCH_DB.MAPLEMONK.store_replen_5_May2025 S5 JOIN SNITCH_DB.MAPLEMONK.store_replen_2_May2025 S2 ON S5.SKU_CODE = S2.logicusercode AND S5.BRANCH_CODE = S2.BRANCH_CODE LEFT JOIN Agg_L L ON S5.SKU_CODE = L.SKU_CODE AND S5.BRANCH_CODE = L.BRANCH_CODE LEFT JOIN Agg_JIT J ON S5.SKU_CODE = J.SKU_CODE AND S5.BRANCH_CODE = J.BRANCH_CODE WHERE NOT (S2.FINAL_ROS < 0.6 AND (S5.SOCIALISTIC + COALESCE(L.total_stock_qty, 0) + COALESCE(J.total_jit_qty, 0)) > 6);",
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
            