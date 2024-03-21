{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SOLARA_DB.MAPLEMONK.SOLARA_DB_MARKETING_CONSOLIDATED_INTERMEDIATE AS select upper(ACCOUNT_NAME) ACCOUNT_NAME ,ACCOUNT_ID ,upper(CAMPAIGN_NAME) CAMPAIGN_NAME ,CAMPAIGN_ID ,upper(ADSET_NAME) ADSET_NAME ,ADSET_ID ,AD_ID ,upper(AD_NAME) AD_NAME ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,DAYNAME(DATE) Day_of_Week ,YEAR(DATE) AS YEAR1 ,MONTH(DATE) AS MONTH1 ,upper(CHANNEL) CHANNEL ,upper(ACCOUNT) ACCOUNT ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from SOLARA_DB.MAPLEMONK.SOLARA_DB_FACEBOOK_CONSOLIDATED union select upper(ACCOUNT_NAME) ACCOUNT_NAME ,ACCOUNT_ID ,upper(CAMPAIGN_NAME) CAMPAIGN_NAME ,CAMPAIGN_ID ,upper(ADSET_NAME) ADSET_NAME ,ADSET_ID ,AD_ID ,upper(AD_NAME) AD_NAME ,Date ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_FINAL_URL ,Day_of_Week ,YEAR1 ,MONTH1 ,upper(CHANNEL) CHANNEL ,upper(ACCOUNT) ACCOUNT ,clicks ,spend ,impressions ,conversions ,conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from SOLARA_DB.MAPLEMONK.SOLARA_DB_GOOGLEADS_CONSOLIDATED Union Select NULL AS ACCOUNT_NAME ,PROFILEID ,upper(CAMPAIGNNAME) CAMPAIGN_NAME ,CAMPAIGNID ,upper(adGroupName) adGroupName ,adGroupId ,ADID ,NULL AS AD_NAME ,DATE ,CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,dayname(try_to_date(date)) DAY_OF_WEEK ,year(try_to_date(date)) YEAR1 ,month(try_to_date(date)) MONTH1 ,\'AMAZON\' AS CHANNEL ,NULL AS ACCOUNT ,CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,AdSales ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM SOLARA_DB.MAPLEMONK.SOLARA_DB_AMAZONADS_MARKETING ; CREATE OR REPLACE TABLE SOLARA_DB.MAPLEMONK.SOLARA_DB_MARKETING_CONSOLIDATED AS select a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,coalesce(a.DATE,b.date) date ,a.AD_TYPE ,a.AD_STRENGTH ,a.AD_NETWORK_TYPE ,a.AD_URL ,a.DAY_OF_WEEK ,a.YEAR1 ,a.MONTH1 ,upper(coalesce(a.CHANNEL, b.channel)) Channel ,a.ACCOUNT ,a.CLICKS ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,a.CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE ,b.campaign_name shopify_campaign_name ,div0(shopify_revenue, count(1) over (partition by coalesce(a.date,b.date) ,lower(coalesce(a.campaign_name,b.campaign_name)) ,lower(coalesce(a.channel,b.channel)) ) ) shopify_revenue from SOLARA_DB.MAPLEMONK.SOLARA_DB_MARKETING_CONSOLIDATED_INTERMEDIATE a full outer join (select order_timestamp::date date ,final_utm_campaign campaign_name ,upper(case when lower(final_utm_channel) like any (\'%paid social%\',\'%facebook%\', \'%instagram%\',\'%ig%\',\'%fb%\') then \'Facebook\' when lower(final_utm_channel) like (\'%google%\') then \'Google\' end) channel ,sum(total_sales) shopify_revenue from SOLARA_DB.MAPLEMONK.SOLARA_DB_SHOPIFY_FACT_ITEMS where channel is not null group by 1,2,3 ) b on lower(a.campaign_name) = lower(b.campaign_name) and a.date = b.date and lower(a.channel) = lower(b.channel);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SOLARA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        