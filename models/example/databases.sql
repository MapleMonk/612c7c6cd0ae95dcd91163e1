{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_flipkart_seller_pla_pca_fact_items; create table public.anveshan_flipkart_seller_pla_pca_fact_items as select ACCOUNT_ID ,\"campaign id\" as CAMPAIGN_NAME ,\"campaign id\" as CAMPAIGN_ID ,\"campaign type\" as Campaign_Type ,\"budgeting type\" as Campaign_Budgeting_Type ,\"campaign budget\" as CAMPAIGN_BUDGET ,\"campaign status\" as CAMPAIGN_STATUS ,to_date(start_time, \'YYYY-MM-DD\')::date as DATE ,\'FLIPKART\' as Channel ,\'FLIPKART SELLER PLA\' as Account ,cast(clicks as bigint) Clicks ,cast(\"Ad Spend\" as DOUBLE PRECISION) Spend ,cast(views as bigint) Views ,ROI ,cast(\"total converted units\" as bigint) Total_Converted_units ,cast(\"total revenue (rs.)\" as DOUBLE PRECISION) Total_Revenue from public.anveshan_flipkart_ads_seller_portal_pla UNION ALL select ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,cast(null as varchar) Campaign_Type ,\"campaign budget type\" as Campaign_Budget_Type ,Campaign_Budget CAMPAIGN_BUDGET ,CAMPAIGN_STATUS ,to_date(start_time, \'YYYY-MM-DD\')::date as DATE ,\'FLIPKART\' as Channel ,\'FLIPKART SELLER PCA\' as Account ,cast(clicks as bigint) Clicks ,cast(campaign_spend as DOUBLE PRECISION) Spend ,cast(views as bigint) Views ,\"direct roi\" as roi ,(cast(\"direct units\" as bigint) + cast(\"indirect units\" as bigint)) Total_Converted_units ,(cast(\"direct revenue\" as DOUBLE PRECISION) + cast(\"indirect units\" as DOUBLE PRECISION)) Total_Revenue from public.anveshan_flipkart_ads_seller_portal_pca ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            