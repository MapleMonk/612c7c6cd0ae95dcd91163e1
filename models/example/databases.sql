{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zouk_D2C_CategoryWise_Target_vs_Achievement AS WITH Category_Level_Target AS ( SELECT TRIM(UPPER(t.Category)) AS Product_Category, TRIM(UPPER(t.Collection)) AS Collection, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, SAFE_CAST(REPLACE(t.Targets,\',\',\'\') AS INT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Target, CASE WHEN CURRENT_DATE() > DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY))) AS no_of_days FROM `MapleMonk.Zouk_D2C_Category_Targets` t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ) , Daily_Spends AS ( SELECT TRIM(UPPER(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY, TRIM(UPPER(COLLECTION)) AS Collection, DATE(Date) AS Spend_Date, SUM(IFNULL(Spend, 0)) AS Spend, SUM(IFNULL(quantity,0)) AS Quantity, SUM(IFNULL(Sales,0)) -SUM(IFNULL(Secondary_Trade_Margin,0)) -SUM(IFNULL(Returns,0)) -SUM(IFNULL(gst,0)) + SUM(IFNULL(secondary_TradeMargin_GST,0)) AS Net_Sales FROM maplemonk.zouk_secondary_pandl WHERE DATE(Date) < CURRENT_DATE() AND LOWER(Marketplace) like \'%shopify%\' and lower(marketplace) not like \'%pos%\' AND not(lower(Collection) like any (\'%search%\',\'%test%\',\'%combo%\',\'%others%\',\'%gift%\',\'%foot%\',\'tag\')) GROUP BY 1, 2, 3 ) , Combined_Data AS ( SELECT COALESCE(dt.Target_Date, ds.Spend_Date) AS Date, COALESCE(dt.PRODUCT_CATEGORY, ds.PRODUCT_CATEGORY) AS PRODUCT_CATEGORY, COALESCE(dt.Collection, ds.Collection) AS Collection, ds.Quantity, dt.Target, dt.days_Remaining, dt.no_of_days, COALESCE(ds.Spend, 0) AS Spend, COALESCE(ds.Net_Sales,0) AS Net_Sales FROM Category_Level_Target dt FULL OUTER JOIN Daily_Spends ds ON TRIM(dt.PRODUCT_CATEGORY) = TRIM(ds.PRODUCT_CATEGORY) AND TRIM(dt.Collection) = TRIM(ds.Collection) AND dt.Target_Date = ds.Spend_Date ) SELECT cd.Date, cd.PRODUCT_CATEGORY, cd.Collection, cd.Target, cd.Spend, cd.Quantity, cd.Net_Sales, cd.days_Remaining, cd.no_of_days, FROM Combined_Data cd ORDER BY 1,2",
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
            