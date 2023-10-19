{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists Perfora_DB.MapleMonk.Perfora_DB_AMAZONADS_CONSOLIDATED ( ACCOUNT_NAME varchar ,PROFILEID varchar ,CAMPAIGNNAME varchar ,CAMPAIGNID varchar ,adGroupName varchar ,adGroupId varchar ,ADID varchar ,AD_NAME varchar ,DATE varchar ,CAMPAIGN_TYPE varchar ,AD_STRENGTH varchar ,AD_NETWORK_TYPE varchar ,AD_URL varchar ,DAY_OF_WEEK varchar ,YEAR1 varchar ,MONTH1 varchar ,CHANNEL varchar ,ACCOUNT varchar ,CLICKS varchar ,SPEND varchar ,IMPRESSIONS varchar ,CONVERSIONS varchar ,AdSales varchar ,ADD_TO_CARTS varchar ,ADD_TO_CART_VALUE varchar ,LANDING_PAGE_VIEWS varchar ,Initiate_checkouts varchar ,Initiate_checkouts_value varchar); create table if not exists Perfora_DB.MapleMonk.Perfora_DB_GOOGLEADS_CONSOLIDATED (ACCOUNT_NAME varchar ,ACCOUNT_ID varchar ,CAMPAIGN_NAME varchar ,CAMPAIGN_ID varchar ,ADSET_NAME varchar ,ADSET_ID varchar ,AD_ID varchar ,AD_NAME varchar ,Date varchar ,AD_TYPE varchar ,AD_STRENGTH varchar ,AD_NETWORK_TYPE varchar ,AD_FINAL_URL varchar ,Day_of_Week varchar ,YEAR1 varchar ,MONTH1 varchar ,Channel varchar ,ACCOUNT varchar ,clicks varchar ,spend varchar ,impressions varchar ,conversions varchar ,conversion_value varchar ,Add_to_carts varchar ,Add_to_cart_value varchar ,Landing_page_views varchar ,Initiate_checkouts varchar ,Initiate_checkouts_value varchar); CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.Perfora_DB_MARKETING_CONSOLIDATED AS select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,DAYNAME(DATE) Day_of_Week ,YEAR(DATE) AS YEAR1 ,MONTH(DATE) AS MONTH1 ,CHANNEL ,ACCOUNT ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from Perfora_DB.MapleMonk.Perfora_DB_FACEBOOK_CONSOLIDATED union select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,Date ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_FINAL_URL ,Day_of_Week ,YEAR1 ,MONTH1 ,Channel ,ACCOUNT ,clicks ,spend ,impressions ,conversions ,conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from Perfora_DB.MapleMonk.Perfora_DB_GOOGLEADS_CONSOLIDATED Union Select NULL AS ACCOUNT_NAME ,PROFILEID ,CAMPAIGNNAME ,CAMPAIGNID ,adGroupName ,adGroupId ,ADID ,NULL AS AD_NAME ,DATE ,CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,DAYNAME(DATE) DAY_OF_WEEK ,YEAR(DATE) YEAR1 ,MONTH(DATE) MONTH1 ,upper(marketplace) AS CHANNEL ,NULL AS ACCOUNT ,sum(CLICKS) ,sum(SPEND) ,sum(IMPRESSIONS) ,sum(CONVERSIONS) ,sum(AdSales) ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM Perfora_DB.MapleMonk.Perfora_DB_AMAZONADS_MARKETING group by ACCOUNT_NAME ,PROFILEID ,CAMPAIGNNAME ,CAMPAIGNID ,adGroupName ,adGroupId ,ADID ,AD_NAME ,DATE ,CAMPAIGN_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_URL ,DAYNAME(DATE) ,YEAR(DATE) ,MONTH(DATE) ,CHANNEL ,ACCOUNT ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        