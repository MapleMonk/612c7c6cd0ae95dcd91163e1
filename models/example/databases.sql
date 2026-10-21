{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE IFB_DB.MAPLEMONK.IFB_flipkart_ads_pla_pca_fact_items AS SELECT ACCOUNT_ID ,\"Campaign Name\" AS CAMPAIGN_NAME ,\"Campaign ID\" AS CAMPAIGN_ID ,CAST(NULL AS STRING) AS Campaign_Type ,CAST(NULL AS STRING) AS Campaign_Budgeting_Type ,Campaign_Budget AS CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,CAST(CAST(start_time AS TIMESTAMP_NTZ) AS DATE) AS date ,\'FLIPKART SELLER\' AS Channel ,\'FLIPKART SELLER PLA\' AS Account ,CAST(clicks AS INT) AS Clicks ,CAST(\"Ad Spend\" AS FLOAT) AS Spend ,CAST(\"VIEWS\" AS INT) AS Views ,\"ROI (Direct)\" AS ROI ,CAST(NULL AS INT) AS Total_Converted_units ,CAST(\"Direct Revenue\" AS FLOAT) + CAST(\"Indirect Revenue\" AS FLOAT) AS Total_Revenue FROM IFB_DB.MAPLEMONK.IFB_Flipkart_Ads_pla UNION ALL SELECT ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,CAST(NULL AS STRING) AS Campaign_Type ,CAST(NULL AS STRING) AS Campaign_Budgeting_Type ,Campaign_Budget AS CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,CAST(CAST(start_time AS TIMESTAMP_NTZ) AS DATE) AS date ,\'FLIPKART SELLER\' AS Channel ,\'FLIPKART SELLER PCA\' AS Account ,CAST(clicks AS INT) AS Clicks ,CAST(campaign_spend AS FLOAT) AS Spend ,CAST(views AS INT) AS Views ,\"Direct ROI\" AS ROI ,CAST(\"DIRECT UNITS\" AS INT) + CAST(\"INDIRECT UNITS\" AS INT) AS Total_Converted_units ,CAST(\"DIRECT REVENUE\" AS FLOAT) + CAST(\"INDIRECT REVENUE\" AS FLOAT) AS Total_Revenue FROM IFB_DB.MAPLEMONK.IFB_Flipkart_Ads_pca ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from IFB_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            