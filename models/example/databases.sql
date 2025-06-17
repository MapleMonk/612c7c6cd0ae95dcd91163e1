{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table rashu_db.maplemonk.flipkart_seller_pla_fact_items as select ACCOUNT_ID ,\"Campaign Name\"CAMPAIGN_NAME ,\"Campaign ID\" CAMPAIGN_ID ,\"Campaign Type\" Campaign_Type ,\"Budgeting Type\" Campaign_Budgeting_Type ,\"Campaign Budget\" CAMPAIGN_BUDGET ,\"Campaign Status\" CAMPAIGN_STATUS ,start_time::date date ,dayname(start_time::date) day_of_week ,year(start_time::date) year1 ,month(start_time::date) MONTH1 ,\'FLIPKART\' Channel ,\'FLIPKART SELLER PLA\' Account ,Clicks ,\"Ad Spend\" Spend ,views ,\"Total converted units\" Total_Converted_units ,\"Total Revenue (Rs.)\" Total_Revenue from rashu_db.maplemonk.rashu_toys_seller_portal_pla ; create or replace table rashu_db.maplemonk.flipkart_placement_performance as select \'Placement PLA\' campaign_type, left(start_time,10)::date date, \"Campaign ID\" campaign_id, \"Campaign Name\" campaign_name, NULL adgroup_id, \"AdGroup Name\" adgroup_name, \"Placement Type\" placement_type, \"Direct Units Sold\" units_sold_direct, \"Indirect Units Sold\" units_sold_indirect, \"Direct Revenue\" direct_revenue, \"Indirect Revenue\" indirect_revenue, \"Average CPC\" average_cpc, ROI, views, clicks, \"Ad Spend\" ad_spend, from rashu_db.maplemonk.rashu_toys_seller_portal_placement_performance_pla; create or replace table rashu_db.maplemonk.flipkart_keyword_performance as select left(start_time,10)::date date, \"Campaign ID\" campaign_id, \"Campaign Name\" campaign_name, \"AdGroup ID\" adgroup_id, attributed_keyword, keyword_match_type, views, clicks, \"Average CPC\" average_cpc, \" Direct Units Sold\" as direct_units_sold, \"Indirect Units Sold\" indirect_units_sold, \"Direct Revenue\" direct_revenue, \"Indirect Revenue\" indirect_revenue, \"Click Through Rate in %\" click_through_rate_percent, \"Direct ROI\" direct_roi, roi, \"Direct Conversion Rate in %\" direct_conversion_rate_percent from rashu_db.maplemonk.rashu_toys_seller_portal_keyword_PLA; create or replace table rashu_db.maplemonk.flipkart_search_term as select \'PLA\' campaign_type, left(start_time,10)::date date, \"Campaign ID\" campaign_id, \"Campaign Name\" campaign_name, \"AdGroup Name\" adgroup_name, Query, try_cast(views as float) views, try_cast(clicks as float) clicks, try_cast(\"Ad spend\" as float) ad_spend, \"Average CPC\" average_cpc, \"Direct Units Sold\"::float direct_units_sold, \"Indirect Units Sold\"::float indirect_units_sold, \"Direct Revenue\"::float direct_revenue, \"Indirect Revenue\"::float indirect_revenue, \"Click Through Rate in %\" click_through_rate_percent, roi, \"Direct Conversion Rate in %\" direct_conversion_rate_percent, \"Indirect Conversion Rate in %\" indirect_conversion_rate_percent from rashu_db.maplemonk.RASHU_TOYS_SELLER_PORTAL_SEARCH_TERM_PLA;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from Rashu_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            