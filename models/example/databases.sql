{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.all_sku_groups_ros as ( with overall_sales as ( select date_trunc(\'week\',date) as order_week, sku_group, category, sum(gross_quantity) as quant_sold_week from snitch_db.maplemonk.horizontal_sales_categories group by 1,2,3 ), ranking_sales as ( select *, row_number() over (partition by sku_group order by quant_sold_week desc) as highest_weeks from overall_sales qualify row_number() over (partition by sku_group order by quant_sold_week desc) <= 7 ) select sku_group, avg(quant_sold_week)::int as average_ros from ranking_sales group by 1 );",
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
            