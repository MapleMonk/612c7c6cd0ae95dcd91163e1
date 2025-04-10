{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_D2C_Spends_Performance AS WITH Targets AS ( SELECT DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Date, t.Channel, CAST(REPLACE(t.Target, \',\', \'\') AS FLOAT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Daily_Target FROM MapleMonk.Zouk_Spends_Targets t, UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), Spends_Achievement AS ( SELECT a.Channel, DATE_ADD(DATE(a.Start_Date), INTERVAL n DAY) AS Date, CAST(REPLACE(a.Spends, \',\', \'\') AS FLOAT64) / (DATE_DIFF(DATE(a.End_Date), DATE(a.Start_Date), DAY) + 1) AS Spends FROM MapleMonk.Zouk_Spends_Achievement a, UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(a.End_Date), DATE(a.Start_Date), DAY))) AS n ), DTC_Marketing AS ( SELECT Date, SPEND, CHANNEL, CAMPAIGN_NAME FROM MapleMonk.zouk_MARKETING_CONSOLIDATED_DTC ), Metric_DTC_Meta AS ( SELECT d.Date, \'Meta Spends\' AS Metric, t.Daily_Target AS Target, SUM(CAST(d.SPEND AS FLOAT64)) AS Actual FROM DTC_Marketing d LEFT JOIN Targets t ON d.Date = t.Date AND LOWER(t.Channel) LIKE \'%meta%\' WHERE LOWER(d.CHANNEL) LIKE \'%facebook%\' AND LOWER(d.CAMPAIGN_NAME) NOT LIKE \'%instagram%\' GROUP BY d.Date, t.Daily_Target ), Metric_DTC_Google AS ( SELECT d.Date, \'Google Spends\' AS Metric, t.Daily_Target AS Target, SUM(CAST(d.SPEND AS FLOAT64)) AS Actual FROM DTC_Marketing d LEFT JOIN Targets t ON d.Date = t.Date AND LOWER(t.Channel) LIKE \'%google%\' WHERE LOWER(d.CHANNEL) LIKE \'%google%\' AND LOWER(d.CAMPAIGN_NAME) NOT LIKE \'%pmax%\' GROUP BY d.Date, t.Daily_Target ), Metric_DTC_Retention AS ( SELECT a.Date, \'Retention\' AS Metric, t.Daily_Target AS Target, SUM(CAST(a.Spends AS FLOAT64)) AS Actual FROM Spends_Achievement a LEFT JOIN Targets t ON a.Date = t.Date AND LOWER(t.Channel) LIKE \'%retention%\' WHERE LOWER(a.Channel) LIKE \'%retention%\' GROUP BY a.Date, t.Daily_Target ), Metric_DTC_Shopify AS ( SELECT a.Date, \'Shopify_App\' AS Metric, t.Daily_Target AS Target, SUM(CAST(a.Spends AS FLOAT64)) AS Actual FROM Spends_Achievement a LEFT JOIN Targets t ON a.Date = t.Date AND LOWER(t.Channel) LIKE \'%shopify%\' WHERE LOWER(a.Channel) LIKE \'%shopify%\' GROUP BY a.Date, t.Daily_Target ), Metric_DTC_Affiliates AS ( SELECT a.Date, \'Affiliates\' AS Metric, t.Daily_Target AS Target, SUM(CAST(a.Spends AS FLOAT64)) AS Actual FROM Spends_Achievement a LEFT JOIN Targets t ON a.Date = t.Date AND LOWER(t.Channel) LIKE \'%affiliate%\' WHERE LOWER(a.Channel) LIKE \'%affiliate%\' GROUP BY a.Date, t.Daily_Target ) SELECT * FROM Metric_DTC_Meta UNION ALL SELECT * FROM Metric_DTC_Google UNION ALL SELECT * FROM Metric_DTC_Retention UNION ALL SELECT * FROM Metric_DTC_Shopify UNION ALL SELECT * FROM Metric_DTC_Affiliates ORDER BY Date, Metric;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            