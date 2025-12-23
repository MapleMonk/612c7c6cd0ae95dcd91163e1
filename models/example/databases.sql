{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.myntra_Ads_products_fact_items as select account_id, start_time::date date, campaign_id, campaign_name, campaign_status, adgroup_id, adgroup_name, adgroup_status, product_id, product_name, impressions, clicks, budget_spend spend, total_revenue, units_sold_total, from redtape_db.maplemonk.redtape_myntra_advertisements_consolidated_product ;",
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
            