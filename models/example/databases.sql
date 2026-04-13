{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.matrixstore_flipkart_seller_pla_pca_fact_items as select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,Campaign_Type ,Budgeting_Type Campaign_Budgeting_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART\' Channel ,\'FLIPKART SELLER PLA\' Account ,cast(clicks as int64) Clicks ,cast(Ad_Spend as float64) Spend ,cast(views as int64) Views ,ROI ,cast(Total_converted_units as int64) Total_Converted_units ,cast(Total_Revenue__Rs__ as float64) Total_Revenue from maplemonk.Matrix_Store_Flipkart_Ads_seller_portal_pla UNION ALL select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,cast(null as string) Campaign_Type ,Campaign_Budget_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART\' Channel ,\'FLIPKART SELLER PCA\' Account ,cast(clicks as int64) Clicks ,cast(campaign_spend as float64) Spend ,cast(views as int64) Views ,Direct_ROI ,(cast(DIRECT_UNITS as int64) + cast(INDIRECT_UNITS as int64)) Total_Converted_units ,(cast(DIRECT_REVENUE as float64) + cast(INDIRECT_REVENUE as float64)) Total_Revenue from maplemonk.Matrix_Store_Flipkart_Ads_seller_portal_pca ;",
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
            