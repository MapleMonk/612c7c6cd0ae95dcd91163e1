{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.trase_myntra_ads_fact_items as select cast(FORMAT_DATE(\'%Y-%-m-%d\', PARSE_DATE(\'%d-%m-%Y\', date)) as date) as Date, campaign_id, campaign_name, adgroup_id, adgroup_name, adgroup_targetting_type, product_id, product_name, adgroup_status, campaign_status, cast(impressions as int64) impressions, cast(clicks as int64) clicks, case when ctr <> \'\' then cast(ctr as float64) end ctr, case when cvr <> \'\' then cast(cvr as float64) end cvr, case when avg_cpc <> \'\' then cast(avg_cpc as float64) end avg_cpc, case when budget_spend <> \'\' then cast(budget_spend as float64) end spend, cast(units_sold_direct as int64) units_sold_direct, cast(units_sold_indirect as int64) units_sold_indirect, case when indirect_revenue <> \'\' then cast(direct_revenue as float64) end direct_revenue, case when indirect_revenue <> \'\' then cast(indirect_revenue as float64) end indirect_revenue, case when roi_direct <> \'\' then cast(roi_direct as float64) end roi_direct, case when roi_indirect <> \'\' then cast(roi_indirect as float64) end roi_indirect, sm.new_sku from maplemonk.S3_Buckets_myntra_ads m left join (select new_sku,marketplace_sku from trase-wh.maplemonk.final_sku_master where lower(marketplace) like \'%myntra%\' qualify row_number() over(partition by trim(marketplace_sku) order by 1) = 1 ) sm on replace(cast(sm.marketplace_sku as string),\' \',\'\') = replace(cast(replace(m.product_id,\',\',\'\') as string),\' \',\'\');",
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
            