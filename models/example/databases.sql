{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table MAPLEMONK.CC_WEBSITE_TARGETS_VS_ACTUALS as With Sales as ( select date ,upper(trim(brand)) Brand ,upper(trim(region)) region ,sum(ifnull(total_sales,0))-sum(ifnull(return_value,0))-sum(ifnull(chargeback_amount,0))-sum(ifnull(total_discount,0)) Net_Sales ,sum(ifnull(marketing_spend,0)) marketing_spend from `MAPLEMONK.CC_Sales_Cost_Source_paid_Date` where date < current_date and lower(marketplace) like \'%website%\' group by 1,2,3 ), Targets as ( With data AS ( SELECT PARSE_DATE(\'%d-%m-%Y\', date) AS Start_Date, last_day(PARSE_DATE(\'%d-%m-%Y\', date)) end_date, upper(trim(brand)) Brand, UPPER(TRIM(Market)) AS region, SUM(IFNULL(SAFE_CAST(REPLACE(Target_Sales, \',\', \'\') AS FLOAT64), 0)) AS Target_Value FROM `MAPLEMONK.CC_Website_Targets` GROUP BY 1, 2, 3,4 ) SELECT case when brand = \'ANI\' then \'ANIMIGO\' when brand = \'SHY\' then \'SHYTOBUY\' when brand = \'WW\' then \'WEIGHTWORLD\' else d.Brand end as Brand, d.region, date_generated AS Start_Date, d.Target_Value / (DATE_DIFF(d.end_date, d.start_date, DAY) + 1) AS Actual_Value FROM data d, UNNEST(GENERATE_DATE_ARRAY(d.start_date, d.end_date)) AS date_generated ) select coalesce(s.date,T.Start_Date) Date ,coalesce(s.region,T.region) region ,coalesce(s.brand,T.brand) brand ,ifnull(s.Net_Sales,0) Net_Sales ,ifnull(T.Actual_Value,0) Target_Value ,ifnull(marketing_spend,0) Marketing_spend ,case when last_day(current_date) = last_day(coalesce(s.date,T.Start_Date)) then EXTRACT(DAY FROM current_date -1 ) else EXTRACT(DAY FROM (last_day(coalesce(s.date,T.Start_Date)))) end as multily ,EXTRACT(DAY FROM (last_day(coalesce(s.date,T.Start_Date)))) as last_date ,FORMAT_DATE(\'%Y-%B\', DATE (last_day(coalesce(s.date,T.Start_Date)))) as month ,FORMAT_DATE(\'%Y-%m\', DATE (last_day(coalesce(s.date,T.Start_Date)))) as month1 from Sales S full outer join Targets T on s.date = T.Start_Date and lower(s.region) = lower(t.region) and lower(s.brand) = lower(t.brand)",
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
            