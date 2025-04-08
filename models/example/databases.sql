{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.RAS_MINI_MARKETING_CONSOLIDATED AS select \'Ras Mini\' as SOURCE_NAME ,a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,coalesce(a.DATE,b.date) DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,FORMAT_DATE(\'%A\', a.DATE) Day_of_Week ,EXTRACT(YEAR FROM a.DATE) AS YEAR ,EXTRACT(MONTH FROM a.DATE) AS MONTH ,upper(coalesce((cast(upper(a.CHANNEL) as string)), b.channel)) CHANNEL ,cast(upper(a.ACCOUNT) as string) as ACCOUNT ,a.CLICKS ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,a.CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE ,b.campaign_name shopify_campaign_name ,safe_divide(shopify_revenue, count(1) over (partition by coalesce(a.date,b.date) ,lower(coalesce(a.campaign_name,b.campaign_name)) ,lower(coalesce(a.channel,b.channel)) ) ) shopify_revenue from maplemonk.RAS_mini_facebook_consolidated a full outer join (select cast(order_timestamp as date) date ,final_utm_campaign campaign_name ,upper(case when lower(final_utm_channel) like any (\'%paid social%\',\'%facebook%\', \'%instagram%\',\'%ig%\',\'%fb%\') then \'FACEBOOK\' when lower(final_utm_channel) like (\'%google%\') then \'GOOGLE\' end) channel ,sum(total_sales) shopify_revenue from maplemonk.RAS_mini_SHOPIFY_FACT_ITEMS where final_utm_channel is not null group by 1,2,3 ) b on lower(a.campaign_name) = lower(b.campaign_name) and a.date = b.date and lower(a.channel) = lower(b.channel) where (a.channel is not null or b.channel is not null);",
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
            