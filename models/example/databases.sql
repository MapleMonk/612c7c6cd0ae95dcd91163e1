{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.myntra_Ads_products_fact_items as select account_id, start_time::date date, campaign_id, campaign_name, campaign_status, adgroup_id, adgroup_name, adgroup_status, sum(impressions) impressions, sum(clicks) clicks, sum(ad_spend) spend, sum(total_revenue) total_revenue, sum(units_sold_total) units_sold_total, from (select * from ( select *, row_number() over(partition by campaign_id, account_id, adgroup_id, start_time::date order by ingested_at desc) rw from redtape_db.maplemonk.redtape_myntra_advertisements_consolidated_daily )where rw = 1) group by 1,2,3,4,5,6,7,8 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            