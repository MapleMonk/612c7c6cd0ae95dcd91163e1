{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.myntra_ads_fact_items as select cast(FORMAT_TIMESTAMP(\"%Y-%m-%d\", start_time, \"Asia/Kolkata\") as date) AS start_date, ROI_Indirect, Revenue_in_Currency_Indirect_in_INR, CVR, Units_Sold_InDirect, FORMAT_TIMESTAMP(\"%Y-%m-%d %H:%M:%S %Z\", end_time, \"Asia/Kolkata\") end_date, Units_Sold_Direct, CTR, ROI_Direct, Clicks, Revenue_in_Currency_Direct_in_INR, Average_CPC_in_Currency_Cost_Per_Click_in_INR, Impressions, Advertiser_Spend_in_Currency_in_INR, account_id, Product_Name, Views, Campaign_ID, Product_ID, Ad_Group_ID, Campaign_Name, Ad_Group_Name, mm.article_no as sku, SPLIT(REPLACE(article_no, \' \', \'\'), \'-\')[OFFSET(0)] AS style from maplemonk.izf_myntra_ads_consolidated_product_spa ma left join maplemonk.gs_myntra_sku_mapping mm on replace(mm.style_id,\' \',\'\') = cast(ma.product_id as string) union all select null AS start_date, cast(ROI_Indirect as float64) ROI_Indirect, cast(indirect_revenue as float64) Revenue_in_Currency_Indirect_in_INR, cast(CVR as float64) CVR, cast(Units_Sold_InDirect as int64) Units_Sold_InDirect, null end_date, cast(Units_Sold_Direct as int64) Units_Sold_Direct, cast(CTR as float64) CTR, cast(ROI_Direct as float64) ROI_Direct, cast(Clicks as int64) CLicks, cast(direct_revenue as float64) Revenue_in_Currency_Direct_in_INR, cast(avg_cpc as float64) Average_CPC_in_Currency_Cost_Per_Click_in_INR, cast(Impressions as int64) impressions, cast(budget_spend as float64) Advertiser_Spend_in_Currency_in_INR, null account_id, Product_Name, null Views, Campaign_ID, cast(Product_ID as int64) Product_ID, AdGroup_ID, Campaign_Name, null Ad_Group_Name, mm.article_no as sku, SPLIT(REPLACE(article_no, \' \', \'\'), \'-\')[OFFSET(0)] AS style from maplemonk.gs_izf_myntra_ads ma left join maplemonk.gs_myntra_sku_mapping mm on replace(mm.style_id,\' \',\'\') = cast(ma.product_id as string) ;",
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
            