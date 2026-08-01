{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table medmongers_db.maplemonk.medmongers_flipkart_seller_pla_pca_fact_items as select ACCOUNT_ID ,\"Campaign Name\" as CAMPAIGN_NAME ,\"Campaign ID\" as CAMPAIGN_ID ,\"Campaign Type\" as Campaign_Type ,\"Budgeting Type\" as Campaign_Budgeting_Type ,\"Campaign Budget\" CAMPAIGN_BUDGET ,\"Campaign Status\" as CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART\' Channel ,\'FLIPKART SELLER PLA\' Account ,cast(clicks as int) Clicks ,cast(\"Ad Spend\" as float) Spend ,cast(views as int) Views ,ROI ,cast(\"Total converted units\" as int) Total_Converted_units ,cast(\"Total Revenue (Rs.)\" as float) Total_Revenue from medmongers_db.maplemonk.Flipkart_MedMongers_seller_portal_pla UNION ALL select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,cast(null as string) Campaign_Type ,\"Campaign Budget Type\" as Campaign_Budget_Type ,Campaign_Budget as CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,date(cast(cast(start_time as timestamp) as datetime)) date ,\'FLIPKART\' Channel ,\'FLIPKART SELLER PCA\' Account ,cast(clicks as int) Clicks ,cast(campaign_spend as float) Spend ,cast(views as int) Views ,\"Direct ROI\" as Direct_ROI ,(cast(\"DIRECT UNITS\" as int) + cast(\"INDIRECT UNITS\" as int)) Total_Converted_units ,(cast(\"DIRECT REVENUE\" as float) + cast(\"INDIRECT REVENUE\" as float)) Total_Revenue from medmongers_db.maplemonk.Flipkart_MedMongers_seller_portal_pca ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            