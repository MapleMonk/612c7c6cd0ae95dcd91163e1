{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_GOOGLEADS_CONSOLIDATED AS select ad_group_name ADSET_NAME ,ad_group_id as ADSET_ID ,ad_group_ad_ad_id as AD_ID ,cast(NULL as string) as AD_NAME ,\'GOOGLE ADS\' as ACCOUNT_NAME ,NULL as ACCOUNT_ID ,campaign_name as CAMPAIGN_NAME ,campaign_id as CAMPAIGN_ID ,segments_date as DATE ,ad_group_ad_ad_type as AD_TYPE ,ad_group_ad_ad_strength as AD_STRENGTH ,segments_ad_network_type as AD_NETWORK_TYPE ,IF(ARRAY_LENGTH(ad_group_ad_ad_final_urls) > 0, ad_group_ad_ad_final_urls[OFFSET(0)], NULL) as AD_FINAL_URL ,segments_day_of_week as DAY_OF_WEEK ,EXTRACT(YEAR FROM cast(segments_date as date)) AS YEAR ,EXTRACT(MONTH FROM cast(segments_date as date)) AS MONTH ,\'GOOGLE\' Channel ,\'GOOGLE ADS\' ACCOUNT ,SUM(cast (metrics_clicks as FLOAT64)) Clicks ,SUM(cast (metrics_cost_micros as FLOAT64))/1000000 Spend ,SUM(cast (metrics_impressions as FLOAT64)) Impressions ,SUM(cast (metrics_conversions as FLOAT64)) Conversions ,SUM(cast (metrics_conversions_value as FLOAT64)) Conversion_Value from maplemonk.Google_ads_Zouk_Google_ad_group_ad_report group by ad_group_name ,ad_group_id ,ad_group_ad_ad_id ,segments_date ,campaign_name ,campaign_id ,ad_group_ad_ad_type ,ad_group_ad_ad_strength ,segments_ad_network_type ,IF(ARRAY_LENGTH(ad_group_ad_ad_final_urls) > 0, ad_group_ad_ad_final_urls[OFFSET(0)], NULL) ,segments_day_of_week UNION all select NULL ,NULL ,NULL ,cast(NULL as string) ,\'GOOGLE ADS\' as ACCOUNT_NAME ,NULL ,campaign_name ,campaign_id ,segments_date ,NULL ,NULL ,NULL ,NULL ,NULL ,EXTRACT(YEAR FROM cast(segments_date as date)) AS YEAR ,EXTRACT(MONTH FROM cast(segments_date as date)) AS MONTH ,\'GOOGLE\' Channel ,\'GOOGLE ADS\' ACCOUNT ,SUM(cast (metrics_clicks as FLOAT64)) clicks ,SUM(cast (metrics_cost_micros as FLOAT64))/1000000 spend ,SUM(cast (metrics_impressions as FLOAT64)) Impressions ,SUM(cast (metrics_conversions as FLOAT64)) Conversions ,SUM(cast (metrics_conversions_value as FLOAT64)) Conversion_Value from maplemonk.Google_ads_Zouk_Google_campaign_data where campaign_advertising_channel_type in (\'PERFORMANCE_MAX\',\'SMART\') group by campaign_name, campaign_id, segments_date ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            