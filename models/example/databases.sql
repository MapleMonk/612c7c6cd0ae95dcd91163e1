{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE redtape_db.maplemonk.redtape_db_flipkart_product_fsn_fact_items as select account_id ACCOUNT_NAME ,account_id ACCOUNT_ID ,\"Campaign Name\" CAMPAIGN_NAME ,\"Campaign ID\" CAMPAIGN_ID ,\"AdGroup Name\" ADSET_NAME ,null ADSET_ID ,null AD_ID ,null AD_NAME ,start_time::date date ,null AD_TYPE ,null AD_STRENGTH ,null AD_NETWORK_TYPE ,null AD_URL ,dayname(start_time::date) day_of_week ,year(start_time::date) year1 ,month(start_time::date) MONTH1 ,\'FLIPKART\' Channel ,\'FLIPKART FSN PLA\' Account ,Clicks ,\"Ad Spend\" Spend ,views ,\"Units Sold (Direct)\"::float + \"Units Sold (Indirect)\"::float total_units_sold ,\"Direct Revenue\"::float + \"Indirect Revenue\"::float total_revenue ,null Add_to_carts ,null Add_to_cart_value ,null Initiate_checkouts ,null Initiate_checkouts_value from maplemonk.redtape_flipkart_consolidated_fsn_pla; CREATE OR REPLACE TABLE redtape_db.Maplemonk.redtape_db_PRODUCT_MARKETING_CONSOLIDATED as select \'FLIPKART FSN REDTAPE\' AS ACCOUNT_NAME ,account_id PROFILEID ,upper(CAMPAIGN_NAME) campaign_name ,CAMPAIGN_ID ,upper(adset_name) adGroupName ,adset_id adGroup_Id ,null ADID ,NULL AS AD_NAME ,DATE ,null CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,DAY_OF_WEEK ,YEAR1 ,MONTH1 ,CHANNEL ,ACCOUNT ,CLICKS ,null as link_clicks ,SPEND ,views IMPRESSIONS ,total_units_sold CONVERSIONS ,total_revenue CONVERSION_VALUE ,NULL AS ADD_TO_CARTS ,NULL AS ADD_TO_CART_VALUE ,NULL AS LANDING_PAGE_VIEWS ,null as Initiate_checkouts ,null as Initiate_checkouts_value from redtape_db.maplemonk.redtape_db_flipkart_product_fsn_fact_items ;",
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
            