{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS maplemonk.TEKKITAKE_AMAZON_ADS_HOURLY_DATA_CONSOLIDATED ( PROFILEID string ,CAMPAIGNNAME string ,CAMPAIGN_ID string ,adGroupName string ,AD_GROUP_ID string ,AD_ID string ,ASIN string ,DATE DATE ,AD_TYPE_1 string ,ACCOUNTYPE string ,DAY_OF_WEEK string ,YEAR int64 ,MONTH string ,SELLERID string ,CLICKS int64 ,SPEND float64 ,IMPRESSIONS int64 ,ATTRIBUTED_CONVERSIONS_14D int64 ,ATTRIBUTED_SALES_14D float64 ); CREATE OR REPLACE TABLE maplemonk.TEKKITAKE_MARKETING_CONSOLIDATED_INTERMEDIATE AS select \'AMAZON\' AS ACCOUNT_NAME ,cast(PROFILEID as string) ACCOUNT_ID ,cast(upper(CAMPAIGNNAME) as string) CAMPAIGN_NAME ,cast(CAMPAIGN_ID as string) CAMPAIGN_ID ,cast(upper(adGroupName) as string) adset_Name ,cast(AD_GROUP_ID as string) ADSET_ID ,cast(AD_ID as string) AD_ID ,ASIN AS AD_NAME ,DATE(CAST(date AS TIMESTAMP)) DATE ,cast(AD_TYPE_1 as string) AD_TYPE ,cast(NULL as string) AS AD_STRENGTH ,ACCOUNTYPE AS AD_NETWORK_TYPE ,cast(NULL as string) AS AD_URL ,FORMAT_DATE(\'%A\', DATE) DAY_OF_WEEK ,EXTRACT(YEAR FROM DATE) YEAR ,EXTRACT(MONTH FROM DATE) MONTH ,\'AMAZON\' AS CHANNEL ,SELLERID AS ACCOUNT ,sum(CLICKS) CLICKS ,sum(SPEND) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(ATTRIBUTED_CONVERSIONS_14D) CONVERSIONS ,SUM(ATTRIBUTED_SALES_14D) ADSALES ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM maplemonk.TEKKITAKE_AMAZON_ADS_HOURLY_DATA_CONSOLIDATED hd group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 ; CREATE OR REPLACE TABLE maplemonk.TEKKITAKE_MARKETING_CONSOLIDATED AS select a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,DATE ,a.AD_TYPE ,a.AD_STRENGTH ,a.AD_NETWORK_TYPE ,a.AD_URL ,a.DAY_OF_WEEK ,a.YEAR ,a.MONTH ,upper(a.CHANNEL) CHANNEL ,a.ACCOUNT ,a.CLICKS ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,null as CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE ,null shopify_revenue from maplemonk.TEKKITAKE_MARKETING_CONSOLIDATED_INTERMEDIATE a ;",
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
            