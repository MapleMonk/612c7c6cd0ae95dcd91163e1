{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.plus_spends_roas as with spends as ( select date, ifnull(sum(case when (lower(ad_name) like \'%plus%\' or lower(adset_name) like \'%plus%\' or (lower(campaign_name) like \'%plus%\') then spend end)::int,0) from snitch_db.maplemonk.marketing_consolidated_snitch group by 1 ), sales as ( select date as date, sum(gross_sales) as sales from snitch_db.maplemonk.horizontal_sales_categories a left join snitch_db.maplemonk.base_product b on a.sku_group = b.sku_group where b.l1_category = \'plus\' and type =\'Shopify\' group by 1 ) select a.*, b.sales, div0(sales,spend) as plus_roas from spends a left join sales b on a.date = b.date ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            