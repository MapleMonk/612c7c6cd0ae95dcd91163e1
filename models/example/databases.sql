{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.Google_ADS_Campaign_Consolidated_Vahdam as select \'Shopify_USA\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_US_CAMPAIGN_DATA union all select \'Shopify_Germany\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_GERMANY_CAMPAIGN_DATA union all select \'Shopify_Global\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_GLOBAL_CAMPAIGN_DATA union all select \'Shopify_India\' as Region ,\"campaign.id\" ,\"campaign.name\" ,\"segments.date\" ,\"metrics.clicks\" ,\"campaign.status\" ,\"metrics.conversions\" ,\"metrics.cost_micros\" ,\"metrics.impressions\" ,\"metrics.conversions_value\" ,\"campaign.advertising_channel_type\" from VAHDAM_DB.MAPLEMONK.gads_in_campaign_data;",
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
                        