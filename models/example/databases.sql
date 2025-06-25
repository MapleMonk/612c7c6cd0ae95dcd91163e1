{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.category_month_index as with cat_month_sales as ( select date_trunc(\'month\',date) as order_month, category, case when lower(sku_group) like \'4mbg%\' then \'plus_size\' when lower(style) like \'%luxe%\' then \'luxe\' when lower(style) like \'%core%\' then \'core\' when lower(category) in (\'perfumes\',\'accessories\',\'bags\',\'belts\',\'sunglasses\',\'shoes\',\'slip-ons\') then \'long_tail\' when lower(style) like \'%revolution%\' then \'revolution\' when lower(style) like \'%bordeaux%\' then \'bordeaux\' else \'Snitch\' end as l1_category, sum(gross_quantity) as sales_qty, SUM(SUM(gross_quantity)) OVER (PARTITION BY category) AS total_12m_sales, sum(SUM(gross_quantity)) over () as total_sales_overall from snitch_db.maplemonk.horizontal_sales_categories where date_trunc(\'month\',date) < date_trunc(\'month\',current_date) and date BETWEEN DATEADD(month, -12, CURRENT_DATE) AND CURRENT_DATE group by 1,2,3 ), cat_calculation_pre as ( select category, l1_category, order_month, round((sales_qty/total_12m_sales)*100,2) as month_category_split, round((total_12m_sales/total_sales_overall)*100,2) as category_split from cat_month_sales ), avg_split as ( select category, l1_category, avg(month_category_split) as avg_category_split from cat_calculation_pre group by 1,2 ) select a.category, a.l1_category, ORDER_MONTH, MONTH(order_month) AS MONTH, div0(avg_category_split,month_category_split) as multiplier, category_split from cat_calculation_pre a left join avg_split b on a.category = b.category and a.l1_category = b.l1_category where a.category is not null ;",
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
            