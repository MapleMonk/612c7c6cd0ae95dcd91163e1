{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table MapleMonk.Saadaa_Zepto_Ads_Fact_Items as SELECT cast(Date as date) AS Date, FORMAT_DATE(\'%A\',cast(date as DATE)) as Day_of_Week, EXTRACT(YEAR FROM cast(date as date)) as YEAR, EXTRACT(MONTH FROM cast(date as date)) as MONTH, \'ZEPTO\' CHANNEL, \'ZEPTO\' ACCOUNT, BrandID, BrandName, ProductID, Campaign_id, ProductName, Campaign_name, Category, sum(CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64)) AS Impressions, sum(CAST(REPLACE(Revenue, \',\', \'\') AS FLOAT64)) AS Revenue, sum(CAST(REPLACE(Spend, \',\', \'\') AS FLOAT64)) AS Spend, avg(CAST(REPLACE(Roas, \',\', \'\') AS FLOAT64)) AS Roas, avg(CAST(REPLACE(Ctr, \',\', \'\') AS FLOAT64)) AS Ctr, avg(CAST(REPLACE(Cpm, \',\', \'\') AS FLOAT64)) AS Cpc, avg(CAST(REPLACE(Atc, \',\', \'\') AS FLOAT64)) AS Atc, sum(CAST(REPLACE(Clicks, \',\', \'\') AS FLOAT64)) AS Clicks, sum(CAST(REPLACE(Orders, \',\', \'\') AS FLOAT64)) AS Orders FROM `MapleMonk.Saadaa_Zepto_Sponsored_products` GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13 ;",
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
            