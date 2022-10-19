{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE DAPOBER653_DB.Maplemonk.DAPOBER653_DB_MARKETING_CONSOLIDATED AS select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,DAYNAME(DATE) Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,ACCOUNT ,Clicks ,Spend ,Impressions ,Conversions ,Conversion_Value ,Add_to_carts ,Add_to_cart_value ,Landing_page_views ,Initiate_checkouts ,Initiate_checkouts_value from DAPOBER653_DB.Maplemonk.DAPOBER653_DB_FACEBOOK_CONSOLIDATED union select ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,Date ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_URL ,Day_of_Week ,YEAR ,MONTH ,Channel ,ACCOUNT ,clicks ,spend ,impressions ,conversions ,conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from DAPOBER653_DB.Maplemonk.DAPOBER653_DB_GOOGLEADS_CONSOLIDATED",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from DAPOBER653_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        