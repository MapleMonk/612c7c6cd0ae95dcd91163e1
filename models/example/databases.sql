{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.matrixsore_flipkart_fsn_pla_fact_items as select \'FLIPKART\' Channel, \'FLIPKART PLA\' Account, sku_id, AdGroup_ID, AdGroup_Name, Campaign_ID, Campaign_Name, date(cast(start_time as datetime)) as Date, fads.Product_Name, cast(ROI as float64) as ROI, cast(Conversion_Rate as float64) CVR, cast(Direct_Units_Sold as int64) Direct_Units_Sold, cast(Indirect_Units_Sold as int64) Indirect_Units_Sold, cast(Total_Revenue__Rs__ as float64) Total_Revenue, (cast(Conversion_Rate as float64) / 100.0) * CAST(clicks AS FLOAT64) AS conversions, sum(cast(views as int64)) as Views, sum(cast(clicks as int64)) as Clicks, sum(CAST(Total_Revenue__Rs__ AS FLOAT64) / (CAST(ROI AS FLOAT64) + 1)) AS ad_spend from maplemonk.Matrix_Store_Flipkart_Ads_seller_portal_consolidated_fsn_pla fads group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 ; CREATE OR REPLACE TABLE maplemonk.matrixstore_flipkart_daily_FSN_performance as WITH sales AS ( SELECT ORDER_DATE AS date, SKU AS sku, SUM(Final_Sale_Units) AS units, SUM(Final_Sale_Amount) AS sales, SUM(Return_Amount) AS returns, SUM(Cancellation_Amount) AS cancellations FROM maplemonk.matrixstore_flipkart_fact_items_EM GROUP BY 1,2 ), ads AS ( SELECT date, NULL AS sku, SUM(Spend) AS ad_spend, SUM(Total_Revenue) AS ad_sales, SUM(Clicks) AS clicks, SUM(Total_Converted_units) AS conversions FROM maplemonk.matrixstore_flipkart_seller_pla_pca_fact_items GROUP BY 1 ), fsn_ads AS ( SELECT date, sku_id AS sku, SUM(ad_spend) AS ad_spend, SUM(Total_Revenue) AS ad_sales, SUM(clicks) AS clicks, SUM(conversions) AS conversions FROM maplemonk.matrixsore_flipkart_fsn_pla_fact_items GROUP BY 1,2 ) SELECT COALESCE(s.date, f.date) AS date, COALESCE(s.sku, f.sku) AS sku, IFNULL(s.units, 0) AS units, IFNULL(s.sales, 0) AS sales, IFNULL(f.ad_spend, 0) AS ad_spend, IFNULL(f.ad_sales, 0) AS ad_sales, IFNULL(f.clicks, 0) AS clicks, IFNULL(f.conversions, 0) AS conversions, SAFE_DIVIDE(f.ad_spend, f.ad_sales) AS acos, SAFE_DIVIDE(f.ad_sales, f.ad_spend) AS roas FROM sales s FULL OUTER JOIN fsn_ads f ON s.date = f.date AND s.sku = f.sku ;",
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
            