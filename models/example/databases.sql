{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.google_consolidated_ads as ( with google_ads as ( SELECT adset_name, date, campaign_name, case when campaign_name like \'%Search%\' then \'Non-Brand Search\' when campaign_name like \'%SearchBrand%\' then \'Brand Search\' when campaign_name like \'%Performance%\' then \'Performance Max\' when campaign_name like \'%Search_NB%\' then \'Non-Brand Search\' when campaign_name like \'%Video%\' then \'Youtube\' when campaign_name like \'%Demand%\' then \'Demand Gen\' when campaign_name like \'%App%\' then \'App\' end as campaign_type, ad_type, ad_strength, ad_network_type, day_of_week, sum(clicks) as \"Clicks\", sum(impressions) as \"Impressions\", sum(spend) as \"Spends\", sum(add_to_carts) as A2C, sum(conversions) as \"Conversions\", sum(conversion_value) as Revenue FROM snitch_db.maplemonk.marketing_consolidated_snitch where account in (\'Google_Snitch\') group by adset_name, date, campaign_name, ad_type, ad_strength, ad_network_type, day_of_week, campaign_type ), session_timeseries as ( SELECT TO_DATE(DATE,\'YYYYMMDD\') as ga_date, \'web\' AS type, CAMPAIGNNAME, sum(ENGAGEDSESSIONS) as ENGAGEDSESSIONS, SUM(sessions) as sessions, FROM snitch_db.maplemonk.web_region_campaign_sessions where date >= \'20231201\' GROUP BY 1, 2, 3 UNION SELECT TO_DATE(DATE,\'YYYYMMDD\') as ga_date, \'app\' AS type, CAMPAIGNNAME, SUM(sessions) as sessions, sum(ENGAGEDSESSIONS) as ENGAGEDSESSIONS, FROM snitch_db.maplemonk.app_region_campaign_sessions where date >= \'20231201\' GROUP BY 1, 2, 3 ), campaign_sessions as ( select ga_date, campaignname, sum(sessions) as sessions, sum(engagedsessions) as engagedsessions from session_timeseries group by 1,2 ) select a.* ,div0(b.sessions,count(*) over(partition by date,lower(CAMPAIGN_NAME))) as final_sessions ,div0(b.engagedsessions,count(*) over(partition by date,lower(CAMPAIGN_NAME))) as final_engagedsessions from google_ads a left join campaign_sessions b on lower(a.campaign_name) = lower(b.campaignname) and a.date = b.ga_date );",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        