{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE dunatura_db.MAPLEMONK.dunatura_DB_GOOGLEADS_CONSOLIDATED AS select null as ADSET_NAME ,null as ADSET_ID ,null as Ad_ID ,null as ad_name ,\'Google_Ads_Dunatura\' as ACCOUNT_NAME ,null as ACCOUNT_ID ,\"campaign.name\" campaign_name ,\"campaign.id\" campaign_id ,\"segments.date\" date ,null as ad_type ,null as ad_strength ,null as ad_network_type ,null as AD_FINAL_URL ,null as DAY_OF_WEEK ,YEAR(\"segments.date\"::date) YEAR1 ,MONTH(\"segments.date\"::date) MONTH1 ,\'Google Paid\' Channel ,\'Google_Ads_Dunatura\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversion_value from dunatura_DB.MAPLEMONK.Google_ads_GoogleAds_dunatura_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from dunatura_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        