{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE prd_db.justherbs.ga4_pnl AS WITH overall AS ( SELECT TO_DATE(date, \'YYYYMMDD\') AS dt, newUsers AS unique_visitors, sessions AS sessions, screenPageViewsPerSession AS pages_per_session, averageSessionDuration / 60 AS avg_session_duration_mins FROM DATALAKE_DB.JUSTHERBS.JH_GA4_P_L__OVERALL__SNAP ), segment_agg AS ( SELECT TO_DATE(date, \'YYYYMMDD\') AS dt, SUM(CASE WHEN sessionDefaultChannelGroup IN (\'Organic Search\',\'Organic Video\',\'Organic Social\',\'Organic Shopping\') THEN averageSessionDuration * sessions END) / NULLIF(SUM(CASE WHEN sessionDefaultChannelGroup IN (\'Organic Search\',\'Organic Video\',\'Organic Social\',\'Organic Shopping\') THEN sessions END), 0) AS avg_duration_organic_sec, SUM(CASE WHEN sessionDefaultChannelGroup IN (\'Paid Shopping\',\'Paid Search\',\'Paid Social\',\'Paid Other\', \'Paid Video\',\'Display\',\'Cross-network\') THEN averageSessionDuration * sessions END) / NULLIF(SUM(CASE WHEN sessionDefaultChannelGroup IN (\'Paid Shopping\',\'Paid Search\',\'Paid Social\',\'Paid Other\', \'Paid Video\',\'Display\',\'Cross-network\') THEN sessions END), 0) AS avg_duration_paid_sec, SUM(CASE WHEN sessionDefaultChannelGroup = \'Direct\' THEN averageSessionDuration * sessions END) / NULLIF(SUM(CASE WHEN sessionDefaultChannelGroup = \'Direct\' THEN sessions END), 0) AS avg_duration_direct_sec FROM DATALAKE_DB.JUSTHERBS.JH_GA4_P_L__SEGMENTS GROUP BY date ), combined AS ( SELECT o.*, s.avg_duration_organic_sec, s.avg_duration_paid_sec, s.avg_duration_direct_sec FROM overall o LEFT JOIN segment_agg s ON o.dt = s.dt ) SELECT dt AS Date, \'Unique Visitors\' AS Metric, unique_visitors AS Numbers FROM combined UNION ALL SELECT dt, \'Sessions\', sessions FROM combined UNION ALL SELECT dt, \'Pages per sessions\', ROUND(pages_per_session, 2) FROM combined UNION ALL SELECT dt, \'Avg. Session Duration (mins)\', ROUND(avg_session_duration_mins, 2) FROM combined UNION ALL SELECT dt, \'Average session duration organic (sec)\', ROUND(avg_duration_organic_sec, 2) FROM combined UNION ALL SELECT dt, \'Average session duration paid (sec)\', ROUND(avg_duration_paid_sec, 2) FROM combined UNION ALL SELECT dt, \'Average session duration direct (sec)\', ROUND(avg_duration_direct_sec, 2) FROM combined ORDER BY Date, Metric ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            