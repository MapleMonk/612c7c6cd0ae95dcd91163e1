{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.myntra_ads_fact_items as select cast(FORMAT_TIMESTAMP(\"%Y-%m-%d\", start_time, \"Asia/Kolkata\") as date) AS start_date, ROI_Indirect, Revenue_in_Currency_Indirect_in_INR, CVR, Units_Sold_InDirect, FORMAT_TIMESTAMP(\"%Y-%m-%d %H:%M:%S %Z\", end_time, \"Asia/Kolkata\") end_date, Units_Sold_Direct, CTR, ROI_Direct, Clicks, Revenue_in_Currency_Direct_in_INR, Average_CPC_in_Currency_Cost_Per_Click_in_INR, Impressions, Advertiser_Spend_in_Currency_in_INR, account_id, Product_Name, Views, Campaign_ID, Product_ID, Ad_Group_ID, Campaign_Name, Ad_Group_Name, mm.article_no as sku, SPLIT(REPLACE(article_no, \' \', \'\'), \'-\')[OFFSET(0)] AS style from maplemonk.izf_myntra_ads_consolidated_product_spa ma left join maplemonk.gs_myntra_sku_mapping mm on replace(mm.style_id,\' \',\'\') = cast(ma.product_id as string) ;",
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
            