{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.hyuman_meesho_ads_fact_items as select cast(start_date as date) as start_date, catalog_id as ad_id, campaign_id as campaign_id, cast(catalog_roi as float64) as catalog_roi, cast(total_views as int64) as campaign_views, catalog_name as ad_name, cast(total_clicks as int64) as campaign_clicks, cast(total_orders as int64) as campaign_orders, cast(total_revenue as float64) as campaign_revenue, campaign_name, catalog_status, cast(catalog_order_count as int64) as ad_orders, cast(catalog_ads_spend as float64) as ad_spend, cast(catalog_total_clicks as float64) as ad_clicks, cast(catalog_total_views as float64) as ad_views, cast(catalog_revenue as float64) as ad_revenue from maplemonk.meesho_ads_campaign_details_reports qualify row_number() over (partition by catalog_id,campaign_id,date(TIMESTAMP(scraped_date)),DATETIME(cast(_airbyte_normalized_at as datetime)) order by cast(_airbyte_normalized_at as datetime) desc,_airbyte_emitted_at desc) = 1",
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
            