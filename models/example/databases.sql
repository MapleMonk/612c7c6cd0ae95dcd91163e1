{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_myntra_ads_fact_items as select date(FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%Y%m%d\', date))) AS start_date, ROI_Indirect, cast(indirect_revenue as float64) indirect_revenue, safe_cast(cvr as float64)/100 CVR, units_sold_direct, Units_Sold_inDirect, safe_cast(CTR as float64)/100 CTR, ROI_Direct, cast(Clicks as int64) Clicks, cast(direct_revenue as float64) direct_revenue, safe_cast(avg_cpc as float64)/100 Average_CPC, cast(Impressions as int64) Impressions, cast(ad_spend as float64) ad_spend, account_id, Campaign_Name, Campaign_ID, adgroup_status, adgroup_id, adgroup_name, campaign_status from maplemonk.saadaa_myntra_consolidated_daily;",
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
            