{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS medmongers_db.maplemonk.Medmongers_FACEBOOK_CONSOLIDATED ( ACCOUNT_NAME STRING, ACCOUNT_ID STRING, CAMPAIGN_NAME STRING, CAMPAIGN_ID STRING, ADSET_NAME STRING, ADSET_ID STRING, AD_ID STRING, AD_NAME STRING, DATE DATE, AD_TYPE STRING, AD_STRENGTH STRING, AD_NETWORK_TYPE STRING, AD_URL STRING, Day_of_Week STRING, YEAR1 STRING, MONTH1 STRING, CHANNEL STRING, ACCOUNT STRING, Clicks FLOAT, Spend FLOAT, Impressions FLOAT, Conversions FLOAT, Conversion_Value FLOAT, Add_to_carts FLOAT, Add_to_cart_value FLOAT, Landing_page_views FLOAT, Initiate_checkouts FLOAT, Initiate_checkouts_value FLOAT ); CREATE OR REPLACE TABLE medmongers_db.maplemonk.MEDMONGERS_MARKETING_CONSOLIDATED AS select * from ( select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,dayofweek(DATE)::string Day_of_Week ,YEAR(DATE) AS YEAR1 ,MONTH(DATE) AS MONTH1 ,\'FACEBOOK\' as CHANNEL ,ACCOUNT ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from medmongers_db.maplemonk.Medmongers_FACEBOOK_CONSOLIDATED Union Select \'AMAZON MEDMONGERS\' AS ACCOUNT_NAME ,PROFILEID::varchar profile_id ,upper(campaignname) CAMPAIGN_NAME ,CAMPAIGN_ID::varchar CAMPAIGN_ID ,upper(adGroupName) adGroupName ,ad_Group_Id ,AD_ID ,NULL AS AD_NAME ,DATE ,AD_TYPE_1 as CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,dayname(date(date)) DAY_OF_WEEK ,year(date(date)) YEAR1 ,month(date(date)) MONTH1 ,\'AMAZON\' AS CHANNEL ,SELLERID AS ACCOUNT ,sum(CLICKS) CLICKS ,sum(SPEND) SPEND ,sum(IMPRESSIONS) IMPRESSIONS ,sum(ATTRIBUTED_CONVERSIONS_14D) CONVERSIONS ,SUM(ATTRIBUTED_SALES_14D) ADSALES ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value FROM maplemonk.MedMongers_AMAZON_ADS_HOURLY_DATA_CONSOLIDATED group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 ) where not(channel like \'%facebook%\') ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            