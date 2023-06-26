{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.US_MARKETING_CONSOLIDATED AS select ADSET_NAME ,ADSET_ID ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,NULL as AD_TYPE ,NULL as AD_STRENGTH ,NULL as AD_NETWORK_TYPE ,NULL as AD_URL ,CLICKS ,SPEND ,IMPRESSIONS ,UNIQUE_INLINE_LINK_CLICKS ,TOTAL_INLINE_LINK_CLICKS ,UNIQUE_OUTBOUND_CLICKS ,TOTAL_OUTBOUND_CLICKS ,CONVERSIONS ,CONVERSION_VALUE ,CHANNEL ,FACEBOOK_ACCOUNT as ACCOUNT from VAHDAM_DB.MAPLEMONK.FACEBOOK_US_CONSOLIDATED UNION select AD_GROUP_NAME ,AD_GROUP_AD_ID ,NULL ,CAMPAIGN_NAME ,CAMPAIGN_ID ,SEGMENTS_DATE ,AD_GROUP_AD_AD_TYPE ,AD_GROUP_AD_AD_STRENGTH ,SEGMENTS_AD_NETWORK_TYPE ,AD_GROUP_AD_AD_FINAL_URLS ,CLICKS ,OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,NULL ,NULL ,NULL ,CONVERSIONS ,CONVERSION_VALUE ,CHANNEL ,ACCOUNT from vahdam_db.maplemonk.US_GOOGLE_ADS_CONSOLIDATED",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        