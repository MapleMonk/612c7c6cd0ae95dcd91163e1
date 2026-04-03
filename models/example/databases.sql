{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Fytika_db_Product_Flipkart_FSN AS SELECT null AS ADSET_NAME, null AS ADSET_ID, null AS AD_ID, null AS AD_NAME, null AS ACCOUNT_NAME, CAST(Account_ID AS STRING) AS ACCOUNT_ID, CAST(Campaign_Name AS STRING) AS CAMPAIGN_NAME, CAST(Campaign_ID AS STRING) AS CAMPAIGN_ID, COALESCE(sc.product_id, ps.product_id) AS PRODUCT_ID, CAST(SC.VARIANT_ID AS STRING) AS VARIANT_ID, CAST(NULL AS STRING) AS AD_TYPE, CAST(NULL AS STRING) AS AD_STRENGTH, CAST(NULL AS STRING) AS AD_NETWORK_TYPE, CAST(NULL AS STRING) AS AD_FINAL_URL, DATE(TIMESTAMP(start_time)) AS DATE, FORMAT_DATE(\'%A\', DATE(TIMESTAMP(start_time))) AS DAY_OF_WEEK, EXTRACT(YEAR FROM DATE(TIMESTAMP(start_time))) AS YEAR, EXTRACT(MONTH FROM DATE(TIMESTAMP(start_time))) AS MONTH, \'FLIPKART\' AS Channel, \'FLIPKART ADS\' AS ACCOUNT, fc.sku_id AS SKU, CAST(adgroup_name AS STRING) AS PRODUCT_CATEGORY, SUM(cast (clicks as FLOAT64)) AS CLICKS, SUM( CASE WHEN SAFE_CAST(roi AS FLOAT64) IS NULL OR SAFE_CAST(roi AS FLOAT64) = 0 THEN 0 ELSE COALESCE(SAFE_CAST(fc.total_revenue__rs__ AS FLOAT64), SAFE_CAST(ps.sales AS FLOAT64)) / SAFE_CAST(roi AS FLOAT64) END) AS SPEND, sum(CAST(views AS FLOAT64)) AS IMPRESSIONS, sum(CAST(conversion_rate AS FLOAT64)) AS CONVERSIONS, sum(CAST(roi AS FLOAT64)) AS CONVERSION_VALUE, sum(coalesce(CAST(fc.total_revenue__rs__ as float64), cast(ps.SALES AS FLOAT64))) AS SALES FROM `maplemonk.Fytika_flipkart_ads___fsn_seller_portal_consolidated_fsn_pla` fc LEFT JOIN ( SELECT PRODUCT_ID, VARIANT_ID, Product_Category, sku FROM maplemonk.maplemonk_analytics_SHOPIFY_FACT_ITEMS QUALIFY ROW_NUMBER() OVER (PARTITION BY VARIANT_ID ORDER BY Product_Category) = 1 ) sc ON fc.SKU_ID = sc.sku LEFT JOIN ( SELECT order_date, product_id, sku, SUM(IFNULL(selling_price,0)) AS Sales FROM `maplemonk.maplemonk_analytics_sales_consolidated` GROUP BY 1,2,3 ) ps ON CAST(fc.sku_id AS STRING) = CAST(ps.sku AS STRING) and DATE(TIMESTAMP(fc.start_time)) = ps.order_date group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;",
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
            