{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.Google_ADS_Campaign_Consolidated_Vahdam as select \'Shopify_USA\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_US_CAMPAIGN_DATA union all select \'Shopify_Germany\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_GERMANY_CAMPAIGN_DATA union all select \'Shopify_Global\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_GLOBAL_CAMPAIGN_DATA union all select \'Shopify_India\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.gads_in_campaign_data; create or replace table vahdam_db.maplemonk.All_regions_marketing_consolidated_Vahdam as select \'Shopify_USA\' as Region, ADSET_NAME, ADSET_ID, ACCOUNT_NAME, ACCOUNT_ID, CAMPAIGN_NAME, CAMPAIGN_ID, DATE, AD_TYPE, AD_STRENGTH, AD_NETWORK_TYPE, AD_URL, DAY_OF_WEEK, YEAR, MONTH, CHANNEL, ACCOUNT, CLICKS, OUTBOUND_CLICKS, SPEND, IMPRESSIONS, CONVERSIONS, CONVERSION_VALUE from vahdam_db.maplemonk.marketing_consolidated union all select \'Shopify_Germany\' as Region ,ADSET_NAME ,ADSET_ID ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_URL ,DAY_OF_WEEK ,YEAR ,MONTH ,CHANNEL ,ACCOUNT ,CLICKS ,OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE from vahdam_db.maplemonk.germany_marketing_consolidated union all select \'Shopify_Global\' as Region ,ADSET_NAME ,ADSET_ID ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_URL ,DAY_OF_WEEK ,YEAR ,MONTH ,CHANNEL ,ACCOUNT ,CLICKS ,OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE from vahdam_db.maplemonk.global_marketing_consolidated Union all select \'Shopify_India\' as Region ,ADSET_NAME ,ADSET_ID ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,AD_TYPE ,AD_STRENGTH ,AD_NETWORK_TYPE ,AD_URL ,DAY_OF_WEEK ,YEAR ,MONTH ,CHANNEL ,ACCOUNT ,CLICKS ,OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE from vahdam_db.maplemonk.marketing_consolidated_in union all select \'Shopify_Italy\' as Region ,ADSET_NAME ,ADSET_ID ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,Null as AD_TYPE ,Null as AD_STRENGTH ,Null as AD_NETWORK_TYPE ,Null as AD_URL ,Null as DAY_OF_WEEK ,Null as YEAR ,Null as MONTH ,CHANNEL ,FACEBOOK_ACCOUNT as ACCOUNT ,CLICKS ,TOTAL_OUTBOUND_CLICKS as OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE from vahdam_db.maplemonk.facebook_italy_consolidated;",
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
                        