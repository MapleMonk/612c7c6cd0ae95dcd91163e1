{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zouk_D2C_CategoryWise_Target_vs_Achievement AS WITH Category_Level_Target AS ( SELECT TRIM(UPPER(t.Category)) AS Product_Category, TRIM(UPPER(t.Collection)) AS Collection, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, SAFE_CAST(REPLACE(t.Targets,\',\',\'\') AS INT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Target, CASE WHEN CURRENT_DATE() > DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY))) AS no_of_days FROM `MapleMonk.Zouk_D2C_Category_Targets` t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), Daily_Spends AS ( SELECT TRIM(UPPER(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY, CASE WHEN lower(trim(collection)) like any (\'%search%\',\'tag\',\'footwear\') then \'OTHERS\' ELSE trim(upper(Collection)) END AS Collection, Date, sum(Net_Revenue) AS Net_Revenue, sum(quantity) AS Quantity, sum(CM_2) AS CM_2, sum(GM) AS GM, sum(ifnull(brand_spend,0)) - sum(ifnull(spend,0)) - sum(ifnull(other_perf_spend,0)) AS Perf_Spend FROM `MapleMonk.Zouk_DTC_Net_Sales` WHERE DATE(Date) < CURRENT_DATE() GROUP BY 1, 2, 3 ), Combined_Data AS ( SELECT COALESCE(dt.Target_Date, ds.Date) AS Date, COALESCE(dt.PRODUCT_CATEGORY, ds.PRODUCT_CATEGORY) AS PRODUCT_CATEGORY, COALESCE(dt.Collection, ds.Collection) AS Collection, ds.Quantity, dt.Target, dt.days_Remaining, dt.no_of_days, COALESCE(ds.Perf_Spend, 0) AS Perf_Spend, COALESCE(ds.Net_Revenue,0) AS Net_Revenue, COALESCE(ds.CM_2) AS CM_2, COALESCE(ds.GM) AS GM FROM Category_Level_Target dt FULL OUTER JOIN Daily_Spends ds ON TRIM(dt.PRODUCT_CATEGORY) = TRIM(ds.PRODUCT_CATEGORY) AND TRIM(dt.Collection) = TRIM(ds.Collection) AND dt.Target_Date = ds.Date ) SELECT cd.Date, cd.PRODUCT_CATEGORY, CASE WHEN ref.Category IS NOT NULL THEN \'NPD\' ELSE \'CLASSIC\' END AS Bucket, cd.Collection, cd.Target, cd.Perf_Spend, cd.Quantity, cd.Net_Revenue, cd.days_Remaining, cd.no_of_days, cd.CM_2, cd.GM FROM Combined_Data cd LEFT JOIN (SELECT DISTINCT TRIM(UPPER(Category)) as Category FROM `MapleMonk.zouk_npd_Sheet1`) ref ON TRIM(UPPER(cd.PRODUCT_CATEGORY)) = ref.Category ORDER BY 1,2",
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
            