{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table bummer_db.maplemonk.bummer_db_flipkart_ads_fact_items as select Advertiser_name ACCOUNT_NAME ,advertiser_name ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,null ADSET_NAME ,null ADSET_ID ,null AD_ID ,null AD_NAME ,start_time::date date ,null AD_TYPE ,null AD_STRENGTH ,null AD_NETWORK_TYPE ,null AD_URL ,dayname(start_time::date) day_of_week ,year(start_time::date) year1 ,month(start_time::date) MONTH1 ,\'FLIPKART\' Channel ,\'FLIPKART PCA\' Account ,Clicks ,campaign_spend Spend ,views ,\"DIRECT UNITS\" direct_units ,\"DIRECT REVENUE\" direct_revenue ,null Add_to_carts ,null Add_to_cart_value ,null Initiate_checkouts ,null Initiate_checkouts_value from bummer_db.maplemonk.flipkart_bummer_db_pca union all select Advertiser_name ACCOUNT_NAME ,advertiser_name ACCOUNT_ID ,\"Campaign Name\"CAMPAIGN_NAME ,\"Campaign ID\" CAMPAIGN_ID ,null ADSET_NAME ,null ADSET_ID ,null AD_ID ,null AD_NAME ,start_time::date date ,null AD_TYPE ,null AD_STRENGTH ,null AD_NETWORK_TYPE ,null AD_URL ,dayname(start_time::date) day_of_week ,year(start_time::date) year1 ,month(start_time::date) MONTH1 ,\'FLIPKART\' Channel ,\'FLIPKART PLA\' Account ,Clicks ,\"Ad Spend\" Spend ,views ,\"Units Sold (Direct)\" direct_units ,\"Direct Revenue\" direct_revenue ,null Add_to_carts ,null Add_to_cart_value ,null Initiate_checkouts ,null Initiate_checkouts_value from bummer_db.maplemonk.flipkart_bummer_db_pla ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from BUMMER_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            