{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.asaya_flipkart_fsn_pla as select \'FLIPKART\' Channel, \'FLIPKART PLA\' Account, sku_id, AdGroup_ID, AdGroup_Name, Campaign_ID, Campaign_Name, date(cast(start_time as datetime)) as Date, fads.Product_Name, cast(ROI as float64) as ROI, cast(Conversion_Rate as float64) CVR, cast(Direct_Units_Sold as int64) Direct_Units_Sold, cast(Indirect_Units_Sold as int64) Indirect_Units_Sold, cast(Total_Revenue__Rs__ as float64) Total_Revenue, (cast(Conversion_Rate as float64) / 100.0) * CAST(clicks AS FLOAT64) AS conversions, sum(cast(views as int64)) as Views, sum(cast(clicks as int64)) as Clicks, sum(CAST(Total_Revenue__Rs__ AS FLOAT64) / (CAST(ROI AS FLOAT64) + 1)) AS ad_spend from maplemonk.asaya_flipkart_seller_portal_consolidated_fsn_pla fads group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 ;",
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
            