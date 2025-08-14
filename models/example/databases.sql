{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Amazon_Weekly_Brand_Sustenance_Report AS WITH Base AS ( SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\',Reporting_Date) AS Reporting_Date, Search_Query, SAFE_CAST(Search_Query_Score AS INT64) AS Search_Query_Score, SAFE_CAST(Search_Query_Volume AS INT64) AS Search_Query_Volume, SAFE_CAST(Impressions__Total_Count AS INT64) AS Impressions_Total_Count, SAFE_CAST(Impressions__Brand_Count AS INT64) AS Impressions_Brand_Count, SAFE_CAST(Clicks__Total_Count AS INT64) AS Clicks_Total_Count, SAFE_CAST(Clicks__Brand_Count AS INT64) AS Clicks_Brand_Count, SAFE_CAST(Purchases__Total_Count AS INT64) AS Purchases_Total_Count, SAFE_CAST(Purchases__Brand_Count AS INT64) AS Purchases_Brand_Count, CONCAT( FORMAT_DATE(\'%d-%m-%Y\', DATE_SUB(PARSE_DATE(\'%Y-%m-%d\', Reporting_Date), INTERVAL (EXTRACT(DAYOFWEEK FROM PARSE_DATE(\'%Y-%m-%d\', Reporting_Date)) - 1) DAY) ), \' to \', FORMAT_DATE(\'%d-%m-%Y\', PARSE_DATE(\'%Y-%m-%d\', Reporting_Date)) ) AS Week_Range, EXTRACT(WEEK FROM PARSE_DATE(\'%Y-%m-%d\', Reporting_Date)) AS Week_Of_Year, CAST(CEIL(EXTRACT(DAY FROM PARSE_DATE(\'%Y-%m-%d\', Reporting_Date)) / 7) AS INT64) AS Week_Of_Month FROM `MapleMonk.Zouk_Amazon_Weekly_SearchQuery_Upload` am ), Matched AS ( SELECT b.*, COALESCE(s.bucket, \'category\') AS Bucket FROM Base b LEFT JOIN `MapleMonk.Zouk_Sheet1` s ON LOWER(b.`Search_Query`) LIKE CONCAT(\'%\', LOWER(s.search_keyword), \'%\') ) SELECT * FROM Matched;",
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
            