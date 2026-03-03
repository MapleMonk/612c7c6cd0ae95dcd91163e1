{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Misschase_flipkart_seller_pla_pca_fact_items as select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,Campaign_Type ,Budgeting_Type Campaign_Budgeting_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART CLCP\' Channel ,\'FLIPKART SELLER PLA - CLCP\' Account ,cast(clicks as int64) Clicks ,cast(Ad_Spend as float64) Spend ,cast(views as int64) Views ,ROI ,cast(Total_converted_units as int64) Total_Converted_units ,cast(Total_Revenue__Rs__ as float64) Total_Revenue from maplemonk.Flipkart_ads_miss_chase_seller_portal_pla UNION ALL select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,cast(null as string) Campaign_Type ,Campaign_Budget_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART CLCP\' Channel ,\'FLIPKART SELLER PCA - CLCP\' Account ,cast(clicks as int64) Clicks ,cast(campaign_spend as float64) Spend ,cast(views as int64) Views ,Direct_ROI ,(cast(DIRECT_UNITS as int64) + cast(INDIRECT_UNITS as int64)) Total_Converted_units ,(cast(DIRECT_REVENUE as float64) + cast(INDIRECT_REVENUE as float64)) Total_Revenue from maplemonk.Flipkart_ads_miss_chase_seller_portal_pca union all select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,Campaign_Type ,Budgeting_Type Campaign_Budgeting_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART PTPL\' Channel ,\'FLIPKART SELLER PLA - CLCP\' Account ,cast(clicks as int64) Clicks ,cast(Ad_Spend as float64) Spend ,cast(views as int64) Views ,ROI ,cast(Total_converted_units as int64) Total_Converted_units ,cast(Total_Revenue__Rs__ as float64) Total_Revenue from maplemonk.Flipkart_ads_ptpl_fk_seller_portal_pla UNION ALL select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,cast(null as string) Campaign_Type ,Campaign_Budget_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART PTPL\' Channel ,\'FLIPKART SELLER PCA - PTPL\' Account ,cast(clicks as int64) Clicks ,cast(campaign_spend as float64) Spend ,cast(views as int64) Views ,Direct_ROI ,(cast(DIRECT_UNITS as int64) + cast(INDIRECT_UNITS as int64)) Total_Converted_units ,(cast(DIRECT_REVENUE as float64) + cast(INDIRECT_REVENUE as float64)) Total_Revenue from maplemonk.Flipkart_ads_ptpl_fk_seller_portal_pca ;",
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
            