{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.tekkitake_flipkart_fsn_ads as select cast(roi as float64) ROI, cast(views as int64) Views, cast(clicks as int64) Clicks, Sku_Id, AdGroup_ID, account_id, cast(cast(start_time as datetime)as date) as start_time, Campaign_ID, AdGroup_Name, Product_Name, Campaign_Name, cast(Conversion_Rate as float64) Conversion_Rate, cast(Direct_Units_Sold as int64) Direct_Units_Sold, cast(Indirect_Units_Sold as int64) Indirect_Units_Sold, cast(Total_Revenue__Rs__ as float64) Total_Revenue from maplemonk.flipkart_Seller_Ads_seller_portal_consolidated_fsn_pla",
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
            