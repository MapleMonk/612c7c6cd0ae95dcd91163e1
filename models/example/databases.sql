{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists HILO_DEMO_ACCOUNT824_DB.Maplemonk.HILO_DEMO_ACCOUNT824_DB_GOOGLEADS_CONSOLIDATED (ACCOUNT_NAME varchar ,ACCOUNT_ID varchar ,CAMPAIGN_NAME varchar ,CAMPAIGN_ID varchar ,ADSET_NAME varchar ,ADSET_ID varchar ,AD_ID varchar ,AD_NAME varchar ,Date varchar ,AD_TYPE varchar ,AD_STRENGTH varchar ,AD_NETWORK_TYPE varchar ,AD_URL varchar ,Day_of_Week varchar ,YEAR1 varchar ,MONTH1 varchar ,Channel varchar ,ACCOUNT varchar ,clicks varchar ,spend varchar ,impressions varchar ,conversions varchar ,conversion_value varchar ,Add_to_carts varchar ,Add_to_cart_value varchar ,Landing_page_views varchar ,Initiate_checkouts varchar ,Initiate_checkouts_value varchar); CREATE OR REPLACE TABLE HILO_DEMO_ACCOUNT824_DB.Maplemonk.HILO_DEMO_ACCOUNT824_DB_MARKETING_CONSOLIDATED AS select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,DAYNAME(DATE) Day_of_Week ,YEAR(DATE) AS YEAR1 ,MONTH(DATE) AS MONTH1 ,CHANNEL ,ACCOUNT ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from HILO_DEMO_ACCOUNT824_DB.Maplemonk.HILO_DEMO_ACCOUNT824_DB_FACEBOOK_CONSOLIDATED union select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,Date ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,null as AD_URL ,Day_of_Week ,year YEAR1 ,month MONTH1 ,Channel ,ACCOUNT ,clicks ,spend ,impressions ,conversions ,conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from HILO_DEMO_ACCOUNT824_DB.Maplemonk.HILO_DEMO_ACCOUNT824_DB_GOOGLEADS_CONSOLIDATED;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HILO_DEMO_ACCOUNT824_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        