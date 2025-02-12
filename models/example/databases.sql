{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.store_replen_5_feb2025 as WITH SKU_SUMMARY AS ( SELECT SKU_CODE, SUM(TOTAL_REPLEN_REQ) AS CUML_REPLEN_REQ, SUM(ALLOCATED_UNITS) AS CUML_ALLO FROM snitch_db.maplemonk.store_replen_4_feb2025 GROUP BY SKU_CODE ), INTERIM_ALLO AS ( SELECT t.SKU_CODE, t.BRANCH_CODE, t.FINAL_ACTION, t.PARETO, t.PRIORITY, t.FACILITY, t.REPLEN_ORDER, t.TOTAL_REPLEN_REQ, t.ALLOCATED_UNITS, s.CUML_REPLEN_REQ, s.CUML_ALLO, CASE WHEN s.CUML_ALLO >= s.CUML_REPLEN_REQ THEN t.ALLOCATED_UNITS ELSE CEIL(s.CUML_ALLO * t.TOTAL_REPLEN_REQ / s.CUML_REPLEN_REQ) END AS INTERIM_ALLO FROM snitch_db.maplemonk.store_replen_4_feb2025 t INNER JOIN SKU_SUMMARY s ON t.SKU_CODE = s.SKU_CODE ), SOCIALISTIC_ALLO AS ( SELECT i.*, CASE WHEN SUM(INTERIM_ALLO) OVER (PARTITION BY SKU_CODE ORDER BY REPLEN_ORDER ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= CUML_ALLO THEN INTERIM_ALLO ELSE 0 END AS SOCIALISTIC FROM INTERIM_ALLO i ) SELECT * FROM SOCIALISTIC_ALLO; create or replace table snitch_db.maplemonk.offline_master_Daily_Report_1 AS WITH DateCheck AS ( SELECT 1 AS Exist FROM snitch_db.maplemonk.offline_master_Daily_Report_1 WHERE DATE = CURRENT_DATE() LIMIT 1 ) SELECT *, current_date as date FROM snitch_db.maplemonk.offline_master WHEre not EXISTS (SELECT * FROM DateCheck) union all select * FROM snitch_db.maplemonk.offline_master_Daily_Report_1;",
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
            