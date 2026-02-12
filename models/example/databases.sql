{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.samandmarshall_myntra_consolidated_advertisement_fact_items as select date(parse_date(\'%Y%m%d\',date)) Date, safe_cast(ctr as float64) CTR, safe_cast(cvr as float64) CVR, cast(clicks as int64) Clicks, cast(impressions as int64) Impressions, safe_cast(avg_cpc as float64) CPC, safe_cast(ad_spend as float64) Spend, safe_cast(roi_total as float64) Total_ROI, account_id, adgroup_id, adgroup_name, cast(indirect_revenue as float64) as indirect_revenue, cast(direct_revenue as float64) as direct_revenue, cast(total_revenue as float64) as total_revenue, cast(units_sold_total as int64) as conversions, adgroup_targetting_type, campaign_status, adgroup_status, safe_cast(roi_direct as float64) roi_direct, campaign_id, campaign_name from maplemonk.myntra_sm_consolidated_daily;",
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
            