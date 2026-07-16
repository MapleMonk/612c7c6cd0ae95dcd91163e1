{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.replen_scorecard AS WITH mix AS ( SELECT DISTINCT branch_code, weighted_mix_deviation, mix_score FROM snitch_db.maplemonk.replen_mix_score ), qty AS ( SELECT DISTINCT branch_code, weighted_qty_deviation, qty_score FROM snitch_db.maplemonk.replen_qty_score ) SELECT COALESCE(m.branch_code, q.branch_code) AS branch_code, m.weighted_mix_deviation, m.mix_score, q.weighted_qty_deviation, q.qty_score, CASE WHEN m.mix_score = \'Poor\' OR q.qty_score = \'Poor\' THEN \'Poor\' WHEN m.mix_score = \'Perfect\' AND q.qty_score = \'Perfect\' THEN \'Perfect\' ELSE \'Okay\' END AS composite_score FROM mix m FULL OUTER JOIN qty q ON m.branch_code = q.branch_code ORDER BY branch_code;",
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
            