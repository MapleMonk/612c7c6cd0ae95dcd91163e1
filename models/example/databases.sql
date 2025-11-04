{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.myntra_style_advertisement_fact_items as select \'BASIXX\' as brand, start_date, account_id, adgroup_id, adgroup_name, campaign_id, campaign_name, Product_name, product_id, sku, style, spends, CTR, CVR, Clicks, Impressions, CPC, final_campaign_name from maplemonk.basixx_myntra_style_advertisement_fact_items union all select \'IZF\' as brand, start_date, account_id, adgroup_id, adgroup_name, campaign_id, campaign_name, Product_name, product_id, sku, style, spends, CTR, CVR, Clicks, Impressions, CPC, final_campaign_name from maplemonk.izf_myntra_style_advertisement_fact_items; create or replace table maplemonk.myntra_placement_advertisement_fact_items as select \'BASIXX\' as brand, start_date, account_id, adgroup_id, adgroup_name, campaign_id, campaign_name, placement, spends, CTR, CVR, Clicks, Impressions, CPC, final_campaign_name from maplemonk.basixx_myntra_placement_advertisement_fact_items union all select \'IZF\' as brand, start_date, account_id, adgroup_id, adgroup_name, campaign_id, campaign_name, placement, spends, CTR, CVR, Clicks, Impressions, CPC, final_campaign_name from maplemonk.izf_myntra_placement_advertisement_fact_items; create or replace table maplemonk.myntra_consolidated_advertisement_fact_items as select brand, Date, CTR, CVR, Clicks, Impressions, CPC, Spend, Toal_ROI, account_id, adgroup_id, roi_direct, campaign_id, campaign_name, final_campaign_name from maplemonk.basixx_myntra_consolidated_advertisement_fact_items Union all select brand, Date, CTR, CVR, Clicks, Impressions, CPC, Spend, Toal_ROI, account_id, adgroup_id, roi_direct, campaign_id, campaign_name, final_campaign_name from maplemonk.izf_myntra_consolidated_advertisement_fact_items;",
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
            