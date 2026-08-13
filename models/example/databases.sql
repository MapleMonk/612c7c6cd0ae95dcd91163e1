{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.beastlife_GA4_Query as WITH calendar AS ( SELECT PARSE_DATE(\'%Y-%m-%d\', event_date) AS event_date, Event, sub_event, FORMAT_DATE(\'%Y%m%d\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS DateAsInteger, FORMAT_DATE(\'%d\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS DateNumber, EXTRACT(YEAR FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) AS Year, FORMAT_DATE(\'%m\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS MonthNumber, FORMAT_DATE(\'%Y/%m\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS YearMonthNumber, FORMAT_DATE(\'%Y/%b\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS YearMonthShort, FORMAT_DATE(\'%b\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS MonthNameShort, FORMAT_DATE(\'%B\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS MonthNameLong, EXTRACT(DAYOFWEEK FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) AS DayOfWeekNumber, FORMAT_DATE(\'%A\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS DayOfWeek, FORMAT_DATE(\'%a\', PARSE_DATE(\'%Y-%m-%d\', event_date)) AS DayOfWeekShort, CONCAT(\'Q\', CAST(EXTRACT(QUARTER FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) AS STRING)) AS Quarter, CONCAT( FORMAT_DATE(\'%Y\', PARSE_DATE(\'%Y-%m-%d\', event_date)), \'/Q\', CAST(EXTRACT(QUARTER FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) AS STRING) ) AS YearQuarter, CASE WHEN EXTRACT(DAY FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) BETWEEN 1 AND 7 THEN \'W1\' WHEN EXTRACT(DAY FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) BETWEEN 8 AND 14 THEN \'W2\' WHEN EXTRACT(DAY FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) BETWEEN 15 AND 21 THEN \'W3\' WHEN EXTRACT(DAY FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) BETWEEN 22 AND 28 THEN \'W4\' WHEN EXTRACT(DAY FROM PARSE_DATE(\'%Y-%m-%d\', event_date)) >= 29 THEN \'W5\' ELSE \'Error\' END AS WeekPeriod FROM `beastlife-wh-474411.maplemonk.beastlife_db_Calender` ) SELECT f.*, c.*,t2.* FROM `maplemonk.GA_landing_page_funnel_metrics` f LEFT JOIN calendar c ON PARSE_DATE(\'%Y%m%d\', f.date) = c.event_date LEFT JOIN `beastlife-wh-474411.maplemonk.Beastlife_db_Landing_Page` t2 ON f.landingPage = t2.landingPage;",
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
            