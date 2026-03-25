{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.spf_summary_analysis AS WITH base_data AS ( SELECT *, CASE WHEN LOWER(CHANNEL_NAME) = \'prozo\' THEN 4 WHEN LOWER(CHANNEL_NAME) IN (\'ekart\', \'delhivery\', \'bluedart\') THEN 5 WHEN LOWER(CHANNEL_NAME) IN (\'ajio\', \'ajio_sapl_tauru\') THEN 3 WHEN LOWER(CHANNEL_NAME) IN (\'myntra\', \'myntrappmp_sapl_tauru\', \'nykaa\', \'nykaa_fashion_sapl_tauru\', \'nykaa_sapl_tauru\') THEN 7 WHEN LOWER(CHANNEL_NAME) IN (\'flipkart\', \'flipkart_tauru\') THEN 14 WHEN LOWER(CHANNEL_NAME) = \'amazon\' THEN 11 ELSE 10 END AS TAT_DAYS FROM maplemonk.spfclaims_final_new_spf_table_c WHERE RECEIVED_DATE >= \'2025-10-01\' AND RECEIVED_DATE <= CURRENT_DATE - 10 AND QC_BUCKET NOT IN (\'WPR\') AND QC_BUCKET <> \'Good Inventory\' AND CHANNEL_NAME <> \'SHOPIFY\' AND PUTAWAY_CODE IS NOT NULL AND PUTAWAY_CODE <> \'\' ) SELECT TRACKING_NO, RECEIVED_DATE, DATE_TRUNC(\'MONTH\', RECEIVED_DATE) AS RECEIVED_MONTH, CREATED_BY, CASE WHEN CREATED_BY IN ( \'northemp.011@snitch.club\', \'northemp.012@snitch.club\', \'northemp.013@snitch.club\', \'northemp.014@snitch.club\', \'northemp.015@snitch.club\', \'northemp.016@snitch.club\', \'yogesh.s@snitch.com\', \'aquib.m@snitch.com\' ) THEN \'north\' ELSE \'south\' END AS WAREHOUSE, CHANNEL_NAME, QC_BUCKET, PUTAWAY_CODE, ELIGIBLE_TO_RAISE_FLAG, TICKET_RAISED_FLAG, TICKET_RAISED_DATE, TICKET_STATUS_BUCKET, TAT_DAYS, 1 AS TOTAL_BAD_INVENTORY, CASE WHEN (ELIGIBLE_TO_RAISE_FLAG = \'Eligible\' AND TICKET_RAISED_FLAG = 1) OR TICKET_STATUS_BUCKET = \'Approved\' THEN 1 ELSE 0 END AS RAISED_FLAG, CASE WHEN ( ELIGIBLE_TO_RAISE_FLAG = \'Eligible\' AND TICKET_RAISED_FLAG = 1 AND DATEDIFF(DAY, RECEIVED_DATE, TICKET_RAISED_DATE) <= TAT_DAYS ) OR TICKET_STATUS_BUCKET = \'Approved\' THEN 1 ELSE 0 END AS RAISED_WITHIN_TAT_FLAG, CASE WHEN TICKET_STATUS_BUCKET = \'Approved\' THEN 1 ELSE 0 END AS APPROVED_FLAG, CASE WHEN TICKET_STATUS_BUCKET = \'Rejected\' THEN 1 ELSE 0 END AS REJECTED_FLAG FROM base_data;",
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
            