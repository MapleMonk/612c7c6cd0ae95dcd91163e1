{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.flipkart_fact_items as select distinct account_id ACCOUNT_NAME ,account_id ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,null ADSET_NAME ,null ADSET_ID ,null AD_ID ,null AD_NAME ,start_time::date date ,null AD_TYPE ,null AD_STRENGTH ,null AD_NETWORK_TYPE ,null AD_URL ,dayname(start_time::date) day_of_week ,year(start_time::date) year1 ,month(start_time::date) MONTH1 ,\'FLIPKART\' Channel ,\'FLIPKART PCA\' Account ,Clicks ,campaign_spend Spend ,views ,\"DIRECT UNITS\"::float + \"INDIRECT UNITS\"::float total_units_sold ,\"DIRECT REVENUE\"::float + \"INDIRECT REVENUE\"::float total_revenue ,null Add_to_carts ,null Add_to_cart_value ,null Initiate_checkouts ,null Initiate_checkouts_value from redtape_db.maplemonk.flipkart_ads_redtape_pca union all select distinct account_id ACCOUNT_NAME ,account_id ACCOUNT_ID ,\"Campaign Name\"CAMPAIGN_NAME ,\"Campaign ID\" CAMPAIGN_ID ,null ADSET_NAME ,null ADSET_ID ,null AD_ID ,null AD_NAME ,start_time::date date ,null AD_TYPE ,null AD_STRENGTH ,null AD_NETWORK_TYPE ,null AD_URL ,dayname(start_time::date) day_of_week ,year(start_time::date) year1 ,month(start_time::date) MONTH1 ,\'FLIPKART\' Channel ,\'FLIPKART PLA\' Account ,Clicks ,\"Ad Spend\" Spend ,views ,\"Units Sold (Direct)\"::float + \"Units Sold (Indirect)\"::float total_units_sold ,\"Direct Revenue\"::float + \"Indirect Revenue\"::float total_revenue ,null Add_to_carts ,null Add_to_cart_value ,null Initiate_checkouts ,null Initiate_checkouts_value from redtape_db.maplemonk.flipkart_ads_redtape_pla ; select * from redtape_db.maplemonk.flipkart_ads_redtape_pla",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            