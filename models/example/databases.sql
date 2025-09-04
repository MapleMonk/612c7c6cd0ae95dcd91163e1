{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `Maplemonk.Flipkart_DSR_Fact_Items` AS SELECT \'MOODY\' AS Brand, \'FLIPKART\' AS Marketplace, CAST(FORMAT_DATE(\'%Y-%m-%d\',COALESCE( SAFE.PARSE_DATE(\'%d-%m-%y\', date), SAFE.PARSE_DATE(\'%d/%m/%Y\', date), SAFE.PARSE_DATE(\'%d/%m/%y\', date), SAFE.PARSE_DATE(\'%d-%m-%Y\', date), SAFE.PARSE_DATE(\'%m/%d/%Y\', date), SAFE.PARSE_DATE(\'%m/%d/%y\', date), SAFE.PARSE_DATE(\'%m-%d-%Y\', date), SAFE.PARSE_DATE(\'%m-%d-%y\', date), SAFE.PARSE_DATE(\'%d %b %Y\', date), SAFE.PARSE_DATE(\'%d %B %Y\', date), SAFE.PARSE_DATE(\'%d-%b-%y\', date), SAFE.PARSE_DATE(\'%d-%b-%Y\', date), SAFE.PARSE_DATE(\'%d-%B-%y\', date), SAFE.PARSE_DATE(\'%d-%B-%Y\', date)))as date) AS Date, FORMAT_DATE(\'%A\', PARSE_DATE(\'%d %B %Y\', Date)) AS Day, SAFE_CAST(REGEXP_REPLACE(REPLACE(CTR, \'%\', \'\'), r\'\.{2,}\', \'.\') AS FLOAT64) AS CTR, SAFE_CAST(REPLACE(REPLACE(CVR, \',\', \'\'), \'%\', \'\') AS FLOAT64) AS CVR, CAST(REPLACE(Clicks, \',\', \'\') AS INT64) AS Clicks, CAST(REPLACE(Spends, \',\', \'\') AS INT64) AS Spends, CAST(Offiers AS STRING) AS Offers, CAST(REPLACE(REPLACE(Ad_Sales, \',\', \'\'), \'\', \'\') AS FLOAT64) AS Ad_Sales, CAST(REPLACE(REPLACE(MRP_Sale, \',\', \'\'), \'\', \'\') AS FLOAT64) AS MRP_Sales, CAST(REPLACE(Ad_Orders, \',\', \'\') AS FLOAT64) AS Ad_Orders, CAST(REPLACE(Day_Target, \',\', \'\') AS FLOAT64) AS Day_Target, CAST(REPLACE(REPLACE(Discount__, \',\', \'\'), \'%\', \'\') AS FLOAT64) AS Discounts, CAST(REPLACE(Impression, \',\', \'\') AS INT64) AS Impressions, CAST(REPLACE(REPLACE(Total_Sales, \',\', \'\'), \'\', \'\') AS FLOAT64) AS Total_Sales, CAST(REPLACE(Total_Orders, \',\', \'\') AS FLOAT64) AS Total_Orders, CAST(REPLACE(REPLACE(Organic_Sales, \',\', \'\'), \'\', \'\') AS FLOAT64) AS Organic_Sales, CAST(REPLACE(REPLACE(ROAS__on_Ads_, \',\', \'\'), \'\', \'\') AS FLOAT64) AS ROAS_on_Ads, CAST(REPLACE(Organic_orders, \',\', \'\') AS FLOAT64) AS Organic_Orders, CAST(REPLACE(REPLACE(Organic_Sales__, \',\', \'\'), \'%\', \'\') AS FLOAT64) AS Organic_Sales_Check, CAST(Avg_DRR_Checking AS STRING) AS Avg_DRR_Checking, CAST(REPLACE(REPLACE(Spends_Incl__Taxes, \',\', \'\'), \'\', \'\') AS FLOAT64) AS Spends_Incl_Taxes, CAST(REPLACE(REPLACE(ROI_on_total_sales_, \',\', \'\'), \'\', \'\') AS FLOAT64) AS ROI_on_Total_Sales, cast((cast(replace(rc.return__,\'.00%\',\'\') as int64)/100) as float64) as return_perc, cast((cast(replace(rc.comission__,\'.00%\',\'\') as int64)/100) as float64) as commission_perc FROM MAPLEMONK.Google_Sheet_Flipkart_DSR left join `MAPLEMONK.Returns_and_commissions_MKT_return_` rc on replace(lower(rc.mkt_place),\' \',\'\') = \'flipkart\' WHERE Date LIKE \'% %\' AND Date IS NOT NULL AND LOWER(TRIM(Date)) != \'day\';",
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
            