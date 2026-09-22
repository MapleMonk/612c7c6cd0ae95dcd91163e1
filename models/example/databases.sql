{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_swiggy_ads_fact_items; CREATE TABLE public.anveshan_swiggy_ads_fact_items AS with swiggy_data as ( select *, DENSE_RANK() OVER (PARTITION BY cast(metrics_date as date), campaign_name ORDER BY _airbyte_normalized_at DESC) as rank from public.swiggy_ads_granular_reports ), swiggy_ads as ( select cast(metrics_date as date) as date, try_cast(trim(regexp_replace(sw.ecpm, \'[^0-9.]\')) as double precision) as ecpm, try_cast(trim(regexp_replace(sw.ecpc, \'[^0-9.]\')) as double precision) as ecpc, bidding_type::VARCHAR AS AD_GROUP_NAME, TRY_CAST(total_ctr AS FLOAT) AS ctr, try_cast(trim(regexp_replace(sw.total_a2c, \'[^0-9.]\')) as numeric) as total_a2c, try_cast(trim(regexp_replace(sw.total_gmv, \'[^0-9.]\')) as double precision) as total_gmv, try_cast(trim(regexp_replace(sw.total_roi, \'[^0-9.]\')) as double precision) as total_roi, try_cast(trim(regexp_replace(sw.total_budget, \'[^0-9.]\')) as numeric) as total_budget, try_cast(trim(regexp_replace(sw.total_clicks, \'[^0-9.]\')) as double precision) as total_clicks, try_cast(trim(regexp_replace(sw.total_conversions, \'[^0-9.]\')) as double precision) as total_conversions, try_cast(trim(regexp_replace(sw.total_impressions, \'[^0-9.]\')) as numeric) as total_impressions, try_cast(trim(regexp_replace(sw.total_budget_burnt, \'[^0-9.]\')) as numeric) as total_budget_burnt, campaign_name, campaign_id, budget_type, ad_property, account_id, match_type, upper(product_name::varchar) as product_name, upper(sw.keyword) as keyword, upper(sw.city) as city, null::numeric as reach, _airbyte_normalized_at from swiggy_data sw where sw.rank = 1 ) select date, \'SWIGGY\' as channel, \'PRODUCT ADS\' as ad_type, ctr, ecpc, cast(0 as float) as views, AD_GROUP_NAME, total_gmv as ad_sales, total_roi, total_budget, total_clicks as clicks, total_conversions as conversions, total_impressions as impressions, total_budget_burnt as spend, s.campaign_name, campaign_id, budget_type, ad_property, account_id, match_type, keyword, city, product_name, gi.category, gi.type_of_ads, gi.pnl_category from swiggy_ads s left join (select * from (select campaign_name, category, \"type of ads\" as type_of_ads, \"pnl category\" as pnl_category, row_number() over (partition by campaign_name order by category desc) as rw from public.GS_instamart_Campaign_Mapping ) where rw=1 ) gi ON upper(trim(gi.campaign_name)) = upper(s.campaign_name);",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            