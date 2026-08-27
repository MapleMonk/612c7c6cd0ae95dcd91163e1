{{ config(
            materialized='table',
                post_hook={
                    "sql": "with scorecard_daily as ( select cast(date as date) as spend_date, upper(trim(coalesce(nom_category,\'UNCATEGORIZED\'))) as category_key, sum(coalesce(spend,0)) as scorecard_spend from snitch_db.maplemonk.meta_scorecard where date is not null group by 1,2 ), catalogue_daily as ( select cast(spend_date as date) as spend_date, upper(trim(coalesce(category,\'UNCATEGORIZED\'))) as category_key, sum(coalesce(amount_spent_inr,0)) as catalogue_spend from snitch_db.maplemonk.v_meta_product_spends_catalogue where spend_date is not null group by 1,2 ), daily as ( select coalesce(s.spend_date, c.spend_date) as spend_date, coalesce(s.category_key, c.category_key) as category_key, coalesce(s.scorecard_spend,0) as scorecard_spend, coalesce(c.catalogue_spend,0) as catalogue_spend, coalesce(s.scorecard_spend,0) + coalesce(c.catalogue_spend,0) as total_spend from scorecard_daily s full outer join catalogue_daily c on s.spend_date = c.spend_date and s.category_key = c.category_key ), spine as ( select d.spend_date, c.category_key from (select distinct spend_date from daily) d cross join (select distinct category_key from daily) c ) select sp.spend_date as date, sp.category_key as nom_category, coalesce(d.scorecard_spend,0) as scorecard_spend, coalesce(d.catalogue_spend,0) as catalogue_spend, coalesce(d.total_spend,0) as total_spend, sum(coalesce(d.total_spend,0)) over ( partition by sp.category_key order by sp.spend_date rows between unbounded preceding and current row ) as cumulative_spend from spine sp left join daily d on d.spend_date = sp.spend_date and d.category_key = sp.category_key order by nom_category, date; select * from meta_product_total_spends where date = \'2026-08-26\'; select sum(spend), nom_category from meta_scorecard where nom_category in (\'Overshirt\', \'Overshirts\') group by nom_category;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            