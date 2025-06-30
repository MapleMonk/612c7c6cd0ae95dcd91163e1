{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.category_l1category_ros_filter_eoq as with main as ( select sku_group, ros from snitch_db.maplemonk.original_ros_str_eoq ), category as ( select sku_group, category, case when lower(sku_group) like \'4mbg%\' then \'plus_size\' when lower(style) like \'%luxe%\' then \'luxe\' when lower(style) like \'%core%\' then \'core\' when lower(category) in (\'perfumes\',\'accessories\',\'bags\',\'belts\',\'sunglasses\',\'shoes\',\'slip-ons\') then \'long_tail\' when lower(style) like \'%revolution%\' then \'revolution\' when lower(style) like \'%bordeaux%\' then \'bordeaux\' else \'Snitch\' end as l1_category from snitch_db.maplemonk.metafields_data qualify row_number() over (partition by sku_group order by status desc) = 1 ), final_data as ( select a.*, b.category, b.l1_category from main a left join category b on a.sku_group = b.sku_group where b.category is not null ) select category, l1_category, (max(ros))::int as max_ros, (min(ros))::int as min_ros, count(distinct sku_group)as sku_count, (PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ros))::int AS p90original, (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ros))::int AS p75, (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ros))::int AS p50, (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ros))::int AS p25, case when category in (\'Joggers & Trackpants\',\'Cargo Pants\',\'Trousers\',\'Jeans\') then (PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY ros))::int else (PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ros))::int end as p90 from final_data group by 1,2 ;",
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
            