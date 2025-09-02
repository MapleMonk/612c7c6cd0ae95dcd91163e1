{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Wondercare_WH_MARKETING_CONSOLIDATED_INTERMEDIATE AS select \'AMAZON IN\' AS ACCOUNT_NAME ,cast(PROFILEID as string) ACCOUNT_ID ,cast(upper(CAMPAIGNNAME) as string) CAMPAIGN_NAME ,cast(CAMPAIGN_ID as string) CAMPAIGN_ID ,cast(upper(adGroupName) as string) ADSET_NAME ,cast(AD_GROUP_ID as string) ADSET_ID ,cast(AD_ID as string) AD_ID ,ASIN AS AD_NAME ,DATE(CAST(date AS TIMESTAMP)) DATE ,cast(AD_TYPE_1 as string) AD_TYPE ,cast(NULL as string) AS AD_STRENGTH ,ACCOUNTYPE AS AD_NETWORK_TYPE ,cast(NULL as string) AS AD_URL ,FORMAT_DATE(\'%A\', DATE) DAY_OF_WEEK ,EXTRACT(YEAR FROM DATE) YEAR ,EXTRACT(MONTH FROM DATE) MONTH ,\'AMAZON\' AS CHANNEL ,SELLERID AS ACCOUNT ,sum(CLICKS) CLICKS ,sum(SPEND) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(ATTRIBUTED_CONVERSIONS_14D) CONVERSIONS ,SUM(ATTRIBUTED_SALES_14D) Conversion_Value ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM maplemonk.Wondercare_AMAZON_ADS_HOURLY_DATA_CONSOLIDATED hd group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 ; CREATE OR REPLACE TABLE maplemonk.Wondercare_wh_MARKETING_CONSOLIDATED AS select a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,coalesce(a.DATE,b.date) DATE ,a.AD_TYPE ,a.AD_STRENGTH ,a.AD_NETWORK_TYPE ,a.AD_URL ,a.DAY_OF_WEEK ,a.YEAR ,a.MONTH ,upper(coalesce(a.CHANNEL, b.channel)) CHANNEL ,a.ACCOUNT ,a.CLICKS ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,a.CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE ,b.campaign_name shopify_campaign_name ,safe_divide(shopify_revenue, count(1) over (partition by coalesce(a.date,b.date) ,lower(coalesce(a.campaign_name,b.campaign_name)) ,lower(coalesce(a.channel,b.channel)) ) ) shopify_revenue from maplemonk.Wondercare_wh_MARKETING_CONSOLIDATED_INTERMEDIATE a full outer join (select cast(order_timestamp as date) date ,final_utm_campaign campaign_name ,upper(case when lower(final_utm_channel) like any (\'%paid social%\',\'%facebook%\', \'%instagram%\',\'%ig%\',\'%fb%\') then \'FACEBOOK\' when lower(final_utm_channel) like (\'%google%\') then \'GOOGLE\' end) channel ,sum(total_sales) shopify_revenue from maplemonk.Wondercare_wh_SHOPIFY_FACT_ITEMS where final_utm_channel is not null group by 1,2,3 ) b on lower(a.campaign_name) = lower(b.campaign_name) and a.date = b.date and lower(a.channel) = lower(b.channel);",
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
            