{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Philips_db_MARKETING_CONSOLIDATED_INTERMEDIATE AS select \'SWIGGY ADS\' ACCOUNT_NAME ,NULL ACCOUNT_ID ,upper(CAMPAIGN_NAME) CAMPAIGN_NAME ,CAMPAIGN_ID ,NULL ADSET_NAME ,NULL ADSET_ID ,NULL AD_ID ,NULL AD_NAME ,DATE ,AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,DAYNAME(DATE) Day_of_Week ,YEAR(DATE) AS YEAR1 ,MONTH(DATE) AS MONTH1 ,upper(CHANNEL) CHANNEL ,NULL ACCOUNT ,Clicks ,NULL link_clicks ,Spend ,Impressions ,AD_SALES Conversions ,AD_SALES Conversion_Value ,NULL Add_to_carts ,NULL Add_to_cart_value ,NULL Landing_page_views ,NULL Initiate_checkouts ,NULL Initiate_checkouts_value FROM MAPLEMONK.PHILIPS_SWIGGY_ADS_FACT_ITEMS ; CREATE OR REPLACE TABLE Maplemonk.Philips_db_MARKETING_CONSOLIDATED AS select a.ACCOUNT_NAME ,a.ACCOUNT_ID ,a.CAMPAIGN_NAME ,a.CAMPAIGN_ID ,a.ADSET_NAME ,a.ADSET_ID ,a.AD_ID ,a.AD_NAME ,a.DATE date ,a.AD_TYPE ,a.AD_STRENGTH ,a.AD_NETWORK_TYPE ,a.AD_URL ,a.DAY_OF_WEEK ,a.YEAR1 ,a.MONTH1 ,upper(a.CHANNEL) Channel ,a.ACCOUNT ,a.CLICKS ,a.link_clicks ,a.SPEND ,a.IMPRESSIONS ,a.CONVERSIONS ,a.CONVERSION_VALUE ,a.ADD_TO_CARTS ,a.ADD_TO_CART_VALUE ,a.LANDING_PAGE_VIEWS ,a.INITIATE_CHECKOUTS ,a.INITIATE_CHECKOUTS_VALUE from Maplemonk.Philips_db_MARKETING_CONSOLIDATED_INTERMEDIATE a;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from philips_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            