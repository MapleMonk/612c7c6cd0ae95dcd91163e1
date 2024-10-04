{{ config(
            materialized='table',
                post_hook={
                    "sql": "create table if not exists maplemonk.zouk_AMAZONADS_MARKETING ( ACCOUNT_NAME STRING ,PROFILEID STRING ,CAMPAIGNNAME STRING ,CAMPAIGNID STRING ,adGroupName STRING ,adGroupId STRING ,ADID STRING ,AD_NAME STRING ,DATE date ,CAMPAIGN_TYPE STRING ,AD_STRENGTH STRING ,AD_NETWORK_TYPE STRING ,AD_URL STRING ,DAY_OF_WEEK INT64 ,YEAR INT64 ,MONTH INT64 ,CHANNEL STRING ,ACCOUNT STRING ,CLICKS FLOAT64 ,SPEND FLOAT64 ,IMPRESSIONS FLOAT64 ,CONVERSIONS FLOAT64 ,AdSales FLOAT64 ,ADD_TO_CARTS FLOAT64 ,ADD_TO_CART_VALUE FLOAT64 ,LANDING_PAGE_VIEWS FLOAT64 ,Initiate_checkouts FLOAT64 ,Initiate_checkouts_value FLOAT64); create table if not exists maplemonk.zouk_GOOGLEADS_CONSOLIDATED (ACCOUNT_NAME STRING ,ACCOUNT_ID STRING ,CAMPAIGN_NAME STRING ,CAMPAIGN_ID STRING ,ADSET_NAME STRING ,ADSET_ID STRING ,AD_ID STRING ,AD_NAME STRING ,Date date ,AD_TYPE STRING ,AD_STRENGTH STRING ,AD_NETWORK_TYPE STRING ,AD_FINAL_URL STRING ,Day_of_Week STRING ,YEAR STRING ,MONTH STRING ,Channel STRING ,ACCOUNT STRING ,clicks FLOAT64 ,spend FLOAT64 ,impressions FLOAT64 ,conversions FLOAT64 ,conversion_value FLOAT64 ,Add_to_carts FLOAT64 ,Add_to_cart_value FLOAT64 ,Landing_page_views FLOAT64 ,Initiate_checkouts FLOAT64 ,Initiate_checkouts_value FLOAT64); create table if not exists maplemonk.zouk_FACEBOOK_CONSOLIDATED (ACCOUNT_NAME string ,ACCOUNT_ID string ,CAMPAIGN_NAME string ,CAMPAIGN_ID string ,ADSET_NAME string ,ADSET_ID string ,AD_ID string ,AD_NAME string ,DATE date ,AD_TYPE string ,AD_STRENGTH string ,AD_NETWORK_TYPE string ,AD_URL string ,Day_of_Week string ,YEAR string ,MONTH string ,CHANNEL string ,ACCOUNT string ,Clicks FLOAT64 ,Spend FLOAT64 ,Impressions FLOAT64 ,Conversions FLOAT64 ,Conversion_Value FLOAT64 ,Add_to_carts FLOAT64 ,Add_to_cart_value FLOAT64 ,Landing_page_views FLOAT64 ,Initiate_checkouts FLOAT64 ,Initiate_checkouts_value FLOAT64); CREATE OR REPLACE TABLE maplemonk.zouk_MARKETING_CONSOLIDATED_INTERMEDIATE AS select cast(upper(ACCOUNT_NAME) as string) ACCOUNT_NAME ,cast(ACCOUNT_ID as string) ACCOUNT_ID ,cast(upper(CAMPAIGN_NAME) as string) CAMPAIGN_NAME ,cast(CAMPAIGN_ID as string) CAMPAIGN_ID ,cast(upper(ADSET_NAME) as string) ADSET_NAME ,cast(ADSET_ID as string) ADSET_ID ,cast(AD_ID as string) AD_ID ,cast(upper(AD_NAME) as string) AD_NAME ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,FORMAT_DATE(\'%A\', DATE) Day_of_Week ,EXTRACT(YEAR FROM DATE) AS YEAR1 ,EXTRACT(MONTH FROM DATE) AS MONTH1 ,cast(upper(CHANNEL) as string) CHANNEL ,cast(upper(ACCOUNT) as string) ACCOUNT ,category ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from maplemonk.zouk_FACEBOOK_CONSOLIDATED union ALL select upper(cast(ACCOUNT_NAME as string)) ACCOUNT_NAME ,cast(ACCOUNT_ID as string) ACCOUNT_ID ,cast(upper(CAMPAIGN_NAME) as string) CAMPAIGN_NAME ,cast(CAMPAIGN_ID as string) CAMPAIGN_ID ,cast(upper(ADSET_NAME) as string) ADSET_NAME ,cast(ADSET_ID as string) ADSET_ID ,cast(AD_ID as string) AD_ID ,cast(upper(AD_NAME) as string) AD_NAME ,Date ,cast(AD_TYPE as string) AD_TYPE ,cast(AD_STRENGTH as string) AD_STRENGTH ,cast(AD_NETWORK_TYPE as string) AD_NETWORK_TYPE ,cast(AD_FINAL_URL as string) AD_FINAL_URL ,Day_of_Week ,YEAR YEAR1 ,MONTH MONTH1 ,upper(CHANNEL) CHANNEL ,upper(ACCOUNT) ACCOUNT ,category ,clicks ,spend ,impressions ,conversions ,conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from maplemonk.zouk_GOOGLEADS_CONSOLIDATED Union ALL select \'AMAZON\' AS ACCOUNT_NAME ,cast(PROFILEID as string) PROFILEID ,cast(upper(CAMPAIGNNAME) as string) CAMPAIGN_NAME ,cast(CAMPAIGN_ID as string) CAMPAIGNID ,cast(upper(adGroupName) as string) adGroupName ,cast(AD_GROUP_ID as string) AdGroupId ,cast(AD_ID as string) ADID ,ASIN AS AD_NAME ,DATE(CAST(date AS TIMESTAMP)) DATE ,cast(AD_TYPE_1 as string) CAMPAIGN_TYPE ,cast(NULL as string) AS AD_STRENGTH ,ACCOUNTYPE AS AD_NETWORK_TYPE ,cast(NULL as string) AS AD_URL ,FORMAT_DATE(\'%A\', DATE) DAY_OF_WEEK ,EXTRACT(YEAR FROM DATE) YEAR1 ,EXTRACT(MONTH FROM DATE) MONTH1 ,\'AMAZON\' AS CHANNEL ,SELLERID AS ACCOUNT ,category ,sum(CLICKS) CLICKS ,sum(SPEND) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(ATTRIBUTED_CONVERSIONS_14D) CONVERSIONS ,SUM(ATTRIBUTED_SALES_14D) ADSALES ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM maplemonk.zouk_AMAZON_ADS_HOURLY_DATA_CONSOLIDATED hd left join ( select distinct CATEGORY, marketplace_sku from `MapleMonk.FINAL_SKU_MASTER` qualify row_number() over(partition by lower(marketplace_sku) order by 1 desc) = 1 and marketplace_sku is not null and marketplace_sku != \'\' and MARKETPLACE = \'B07LB7TP4M\' ) fsm on lower(fsm.marketplace_sku) = lower(hd.asin) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 UNION ALL SELECT \'MYNTRA\' ACCOUNT_NAME ,NULL as ACCOUNT_ID ,upper(Campaign_Name) CAMPAIGN_NAME ,CAMPAIGN_ID ,upper(Ad_Group_Name) Ad_Group_Name ,Ad_Group_ID ,Product_ID ,upper(Product_Name) Product_Name ,Date ,AD_TYPE ,cast(NULL as string) AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,FORMAT_DATE(\'%A\', DATE) DAY_OF_WEEK ,EXTRACT(YEAR FROM DATE) YEAR1 ,EXTRACT(MONTH FROM DATE) MONTH1 ,CHANNEL ,\'MYNTRA\' ACCOUNT ,null as category ,Clicks ,Advertiser_Spend_in_Currency_in_INR SPEND ,IMPRESSIONS ,(Units_Sold_Direct + Units_Sold_InDirect) Conversions ,(Revenue_in_Currency_Direct_in_INR + Revenue_in_Currency_Indirect_in_INR) AD_SALES ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,Views ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM MAPLEMONK.ZOUK_MYNTRA_ADS_FACT_ITEMS UNION ALL SELECT \'FLIPKART\' ACCOUNT_NAME , NULL as ACCOUNT_ID , upper(Campaign_Name) CAMPAIGN_NAME , CAMPAIGN_ID , upper(Ad_Group_Name) Ad_Group_Name ,Ad_Group_ID ,Product_ID , Product_Name ,Date ,AD_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,FORMAT_DATE(\'%A\', DATE) DAY_OF_WEEK ,EXTRACT(YEAR FROM DATE) YEAR1 ,EXTRACT(MONTH FROM DATE) MONTH1 ,CHANNEL ,\'FLIPKART\' ACCOUNT ,null as category ,SAFE_DIVIDE((DIRECT_UNITS + INDIRECT_UNITS),CVR) Clicks ,AD_SPEND ,SAFE_DIVIDE(SAFE_DIVIDE((DIRECT_UNITS + INDIRECT_UNITS),CVR), CTR) IMPRESSIONS , (DIRECT_UNITS + INDIRECT_UNITS) Conversions , (Direct_Revenue + Indirect_Revenue) AD_SALES ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE , Views ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM MapleMonk.ZOUK_FLIPKART_ADS_FACT_ITEMS UNION ALL SELECT \'SWIGGY_INSTAMART\' ACCOUNT_NAME , NULL as ACCOUNT_ID , upper(Campaign_Name) CAMPAIGN_NAME , CAMPAIGN_ID , null Ad_Group_Name ,null Ad_Group_ID ,null as Product_ID , Product_Name ,Date ,null AD_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,FORMAT_DATE(\'%A\', DATE) DAY_OF_WEEK ,EXTRACT(YEAR FROM DATE) YEAR1 ,EXTRACT(MONTH FROM DATE) MONTH1 ,\'SWIGGY_INSTAMART\' as CHANNEL ,\'SWIGGY_INSTAMART\' ACCOUNT ,null as category ,(Clicks) Clicks ,spend AD_SPEND ,IMPRESSIONS ,0 Conversions , ad_sales AD_SALES ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE , null as Views ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM MapleMonk.ZOUK_SWIGGY_ADS_FACT_ITEMS ; CREATE OR REPLACE TABLE maplemonk.zouk_MARKETING_CONSOLIDATED AS select a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,coalesce(a.DATE,b.date) DATE ,a.AD_TYPE ,a.AD_STRENGTH ,a.AD_NETWORK_TYPE ,a.AD_URL ,a.DAY_OF_WEEK ,a.YEAR1 ,a.MONTH1 ,upper(coalesce(a.CHANNEL, b.channel)) CHANNEL ,a.ACCOUNT ,a.category ,a.CLICKS ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,a.CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE ,b.campaign_name shopify_campaign_name ,safe_divide(shopify_revenue, count(1) over (partition by coalesce(a.date,b.date) ,lower(coalesce(a.campaign_name,b.campaign_name)) ,lower(coalesce(a.channel,b.channel)) ) ) shopify_revenue from maplemonk.zouk_MARKETING_CONSOLIDATED_INTERMEDIATE a full outer join (select cast(order_timestamp as date) date ,final_utm_campaign campaign_name ,upper(case when lower(final_utm_channel) like any (\'%paid social%\',\'%facebook%\', \'%instagram%\',\'%ig%\',\'%fb%\') then \'Facebook\' when lower(final_utm_channel) like (\'%google%\') then \'Google\' end) channel ,sum(total_sales) shopify_revenue from maplemonk.zouk_SHOPIFY_FACT_ITEMS where final_utm_channel is not null group by 1,2,3 ) b on lower(a.campaign_name) = lower(b.campaign_name) and a.date = b.date and lower(a.channel) = lower(b.channel);",
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
            