{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE ANDAMEN_DB.MAPLEMONK.channelwise_COGS_FACT_ITEMS AS WITH actual_metrics AS ( SELECT channel_name, TO_CHAR(COALESCE(transaction_date, return_date), \'YYYY-MM\') AS month, SUM(CASE WHEN transaction_type = \'SALES\' THEN taxable_value + cgst + sgst + igst + cess ELSE 0 END) AS gob_actual, SUM(CASE WHEN transaction_type = \'RETURN\' THEN taxable_value + cgst + sgst + igst + cess ELSE 0 END) AS leakages_actual, SUM(CASE WHEN transaction_type = \'SALES\' THEN taxable_value + cgst + sgst + igst + cess WHEN transaction_type = \'RETURN\' THEN -(taxable_value + cgst + sgst + igst + cess) ELSE 0 END) AS nrv_actual FROM ANDAMEN_DB.MAPLEMONK.COGS_FACT_ITEMS GROUP BY 1, 2 ), targets AS ( SELECT marketplace AS channel_name, metric, CAST(REPLACE(target, \',\', \'\') AS NUMBER) as target_value, month FROM ANDAMEN_DB.MAPLEMONK.ANDAMEN_COGS_CHANNELWISE_TARGETS ) SELECT am.channel_name, t_gob.month, \'GOB\' AS metric, am.gob_actual as actual_value, t_gob.target_value, ROUND((am.gob_actual / nullif(t_gob.target_value,0)) * 100, 2) AS achievement_pct FROM actual_metrics am LEFT JOIN targets t_gob ON am.channel_name = t_gob.channel_name AND t_gob.metric = \'GOB\' UNION ALL SELECT am.channel_name, t_lea.month, \'Leakages\' AS metric, am.leakages_actual, t_lea.target_value, ROUND((am.leakages_actual / nullif(t_lea.target_value,0)) * 100, 2) FROM actual_metrics am LEFT JOIN targets t_lea ON am.channel_name = t_lea.channel_name AND t_lea.metric = \'Leakages\' UNION ALL SELECT am.channel_name, t_nrv.month, \'NRV\' AS metric, am.nrv_actual, t_nrv.target_value, ROUND((am.nrv_actual / nullif(t_nrv.target_value,0)) * 100, 2) FROM actual_metrics am LEFT JOIN targets t_nrv ON am.channel_name = t_nrv.channel_name AND t_nrv.metric = \'NRV\' ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from andamen_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            