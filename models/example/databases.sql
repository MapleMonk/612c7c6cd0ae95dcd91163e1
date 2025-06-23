{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table pronk-wh.maplemonk.pronk_myntra_ads_fact_items as select cast(date as date) as Date, cast(views as int64) as Views, cast(clicks as int64) as Clicks, cast(Ad_Group_ID as string) as Ad_group_ID, cast(Campaign_ID as string) as Campaign_ID, cast(impressions as int64) as Impressions, cast(ROI__Direct_ as float64) as Direct_ROI, cast(Ad_Group_Name as string) as Ad_Group_Name, cast(Campaign_Name as string) as CampaignName, cast(ROI__Indirect_ as float64) as Indirect_ROI, cast(Units_Sold__Direct_ as int64) as Units_sold_direct, cast(Units_Sold__InDirect_ as int64) as Units_solde_indirect, cast(Advertiser_Spend_in_Currency__in_INR_ as float64) as Ad_spend, cast(Revenue_in_Currency__Direct___in_INR_ as float64) as Direct_Revenue, cast(Revenue_in_Currency__Indirect___in_INR_ as float64) as Indirect_Revenue, cast(Average_CPC_in_Currency__Cost_Per_Click___in_INR_ as float64) as CPC, cast(CTR as float64) as CTR, cast(CVR as float64) as CVR from pronk-wh.maplemonk.pronk_s3_myntra_ads;",
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
            