{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table JPEARLS_DB.MAPLEMONK.JPEARLS_DB_FACEBOOK_CONSOLIDATED as select ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,LINK_URL_ASSET ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE ,ADD_TO_CARTS ,ADD_TO_CART_VALUE ,LANDING_PAGE_VIEWS ,INITIATE_CHECKOUTS ,INITIATE_CHECKOUTS_VALUE ,CHANNEL ,ACCOUNT from JPEARLS_DB.MAPLEMONK.JPEARLS_DB_FACEBOOK_CONSOLIDATED_W_URL union select ADSET_NAME ,ADSET_ID ,AD_ID ,AD_NAME ,LINK_URL_ASSET ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE ,ADD_TO_CARTS ,ADD_TO_CART_VALUE ,LANDING_PAGE_VIEWS ,INITIATE_CHECKOUTS ,INITIATE_CHECKOUTS_VALUE ,CHANNEL ,ACCOUNT from JPEARLS_DB.MAPLEMONK.JPEARLS_DB_FACEBOOK_FACT_ITEMS_WO_URL where concat(ad_id, \'-\', date) not in (select concat(ad_id,\'-\',date) from JPEARLS_DB.MAPLEMONK.JPEARLS_DB_FACEBOOK_CONSOLIDATED_W_URL);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from JPEARLS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        