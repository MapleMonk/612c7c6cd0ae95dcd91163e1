{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_blinkit_ads_fact_items; create table public.anveshan_blinkit_ads_fact_items as with blinkit_data as ( select *, DENSE_RANK() OVER (PARTITION BY date, \"campaign name\" ORDER BY _airbyte_normalized_at DESC) as rank from public.anveshan_blinkit_ads_final_ads ), blinkit_ads as ( select coalesce( to_date(trim(ba.date), \'dd-mm-yyyy\'), to_date(trim(ba.date), \'yyyy-mm-dd\'), to_date(trim(ba.date), \'dd/mm/yyyy\') ) as date, try_cast(trim(regexp_replace(ba.\"estimated budget consumed\", \'[^0-9.]\')) as double precision) as estimated_budget_consumed, try_cast(trim(regexp_replace(ba.\"total roas\", \'[^0-9.]\')) as double precision) as total_roas, try_cast(trim(regexp_replace(ba.cpm, \'[^0-9.]\')) as double precision) as cpm, try_cast(trim(regexp_replace(ba.\"direct atc\", \'[^0-9.]\')) as double precision) as direct_atc, try_cast(trim(regexp_replace(ba.\"direct quantities sold\", \'[^0-9.]\')) as numeric) as direct_quantities_sold, try_cast(trim(regexp_replace(ba.\"indirect quantities sold\", \'[^0-9.]\')) as numeric) as indirect_quantities_sold, try_cast(trim(regexp_replace(ba.\"direct sales\", \'[^0-9.]\')) as double precision) as direct_sales, try_cast(trim(regexp_replace(ba.\"indirect sales\", \'[^0-9.]\')) as double precision) as indirect_sales, try_cast(trim(regexp_replace(ba.impressions, \'[^0-9.]\')) as numeric) as impressions, ba.\"campaign name\" as campaign_name, null as collection, try_cast(trim(regexp_replace(ba.\"match type\", \'[^0-9.]\')) as double precision) as ctr, null::numeric as reach, ba.\"targeting type\" as match_type, try_cast(trim(regexp_replace(ba.\"indirect atc\", \'[^0-9.]\')) as numeric) as unique_clicks, ba.\"targeting type\" as targeting_type, ba.\"targeting value\" as targeting_value, report_type, ba._airbyte_normalized_at from blinkit_data ba where ba.rank = 1 ) select date, estimated_budget_consumed, total_roas, cpm, direct_atc, direct_quantities_sold, indirect_quantities_sold, direct_sales, indirect_sales, impressions, b.campaign_name, collection, ctr, reach, match_type, unique_clicks as clicks, targeting_type, targeting_value, report_type, gb.category, gb.type_of_ads, gb.pnl_category, b._airbyte_normalized_at from blinkit_ads b left join (select * from (select \"campaign name\" as campaign_name, \"sub-category\" as category, \"type of ad\" as type_of_ads, \"pnl category\" pnl_category, row_number() over (partition by \"campaign name\" order by \"sub-category\" desc) as rw from public.gs_blinkit_campaign_mapping ) where rw=1 ) gb on upper(trim(gb.campaign_name)) = upper(b.campaign_name);",
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
            