{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists elcinco_db.MAPLEMONK.elcinco_db_AMAZONADS_MARKETING ( ACCOUNT_NAME varchar ,PROFILEID varchar ,CAMPAIGNNAME varchar ,CAMPAIGNID varchar ,adGroupName varchar ,adGroupId varchar ,ADID varchar ,AD_NAME varchar ,DATE date ,CAMPAIGN_TYPE varchar ,AD_STRENGTH varchar ,AD_NETWORK_TYPE varchar ,AD_URL varchar ,DAY_OF_WEEK int ,YEAR1 int ,MONTH1 int ,CHANNEL varchar ,ACCOUNT varchar ,CLICKS float ,SPEND float ,IMPRESSIONS float ,CONVERSIONS float ,AdSales float ,ADD_TO_CARTS float ,ADD_TO_CART_VALUE float ,LANDING_PAGE_VIEWS float ,Initiate_checkouts float ,Initiate_checkouts_value float); create table if not exists elcinco_db.MAPLEMONK.elcinco_db_GOOGLEADS_CONSOLIDATED (ACCOUNT_NAME varchar ,ACCOUNT_ID varchar ,CAMPAIGN_NAME varchar ,CAMPAIGN_ID varchar ,ADSET_NAME varchar ,ADSET_ID varchar ,AD_ID varchar ,AD_NAME varchar ,Date date ,AD_TYPE varchar ,AD_STRENGTH varchar ,AD_NETWORK_TYPE varchar ,AD_FINAL_URL varchar ,Day_of_Week varchar ,YEAR1 varchar ,MONTH1 varchar ,Channel varchar ,ACCOUNT varchar ,clicks float ,spend float ,impressions float ,conversions float ,conversion_value float ,Add_to_carts float ,Add_to_cart_value float ,Landing_page_views float ,Initiate_checkouts float ,Initiate_checkouts_value float); create table if not exists elcinco_db.MAPLEMONK.elcinco_db_FACEBOOK_CONSOLIDATED (ACCOUNT_NAME varchar ,ACCOUNT_ID varchar ,CAMPAIGN_NAME varchar ,CAMPAIGN_ID varchar ,ADSET_NAME varchar ,ADSET_ID varchar ,AD_ID varchar ,AD_NAME varchar ,DATE date ,AD_TYPE varchar ,AD_STRENGTH varchar ,AD_NETWORK_TYPE varchar ,AD_URL varchar ,Day_of_Week varchar ,YEAR1 varchar ,MONTH1 varchar ,CHANNEL varchar ,ACCOUNT varchar ,Clicks float ,Spend float ,Impressions float ,Conversions float ,Conversion_Value float ,Add_to_carts float ,Add_to_cart_value float ,Landing_page_views float ,Initiate_checkouts float ,Initiate_checkouts_value float); CREATE OR REPLACE TABLE elcinco_db.MAPLEMONK.elcinco_db_MARKETING_CONSOLIDATED_INTERMEDIATE AS select upper(ACCOUNT_NAME) ACCOUNT_NAME ,ACCOUNT_ID ,upper(CAMPAIGN_NAME) CAMPAIGN_NAME ,CAMPAIGN_ID ,upper(ADSET_NAME) ADSET_NAME ,ADSET_ID ,AD_ID ,upper(AD_NAME) AD_NAME ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,DAYNAME(DATE) Day_of_Week ,YEAR(DATE) AS YEAR1 ,MONTH(DATE) AS MONTH1 ,upper(CHANNEL) CHANNEL ,upper(ACCOUNT) ACCOUNT ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from elcinco_db.MAPLEMONK.elcinco_db_FACEBOOK_CONSOLIDATED union select upper(ACCOUNT_NAME) ACCOUNT_NAME ,ACCOUNT_ID ,upper(CAMPAIGN_NAME) CAMPAIGN_NAME ,CAMPAIGN_ID ,upper(ADSET_NAME) ADSET_NAME ,ADSET_ID ,AD_ID ,upper(AD_NAME) AD_NAME ,Date ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_FINAL_URL ,Day_of_Week ,YEAR1 ,MONTH1 ,upper(CHANNEL) CHANNEL ,upper(ACCOUNT) ACCOUNT ,clicks ,spend ,impressions ,conversions ,conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from elcinco_db.MAPLEMONK.elcinco_db_GOOGLEADS_CONSOLIDATED Union Select NULL AS ACCOUNT_NAME ,PROFILEID ,upper(CAMPAIGNNAME) CAMPAIGN_NAME ,CAMPAIGNID ,upper(adGroupName) adGroupName ,adGroupId ,ADID ,NULL AS AD_NAME ,DATE ,CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,dayname(try_to_date(date)) DAY_OF_WEEK ,year(try_to_date(date)) YEAR1 ,month(try_to_date(date)) MONTH1 ,\'AMAZON\' AS CHANNEL ,NULL AS ACCOUNT ,CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,AdSales ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM elcinco_db.MAPLEMONK.elcinco_db_AMAZONADS_MARKETING ; CREATE OR REPLACE TABLE elcinco_db.MAPLEMONK.elcinco_db_MARKETING_CONSOLIDATED AS select a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,coalesce(a.DATE,null) date ,a.AD_TYPE ,a.AD_STRENGTH ,a.AD_NETWORK_TYPE ,a.AD_URL ,a.DAY_OF_WEEK ,a.YEAR1 ,a.MONTH1 ,upper(coalesce(a.CHANNEL, null)) Channel ,a.ACCOUNT ,a.CLICKS ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,a.CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE from elcinco_db.MAPLEMONK.elcinco_db_MARKETING_CONSOLIDATED_INTERMEDIATE a ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from elcinco_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        