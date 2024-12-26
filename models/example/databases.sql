{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.demand_supply_category as ( WITH main_data AS ( SELECT date, category::varchar AS category, cost_price::int as cost_price, price::int as price, SUM(IFNULL(gross_quantity, 0)) AS gross_quantity FROM snitch_db.maplemonk.horizontal_sales_categories WHERE LOWER(type) IN (\'shopify\', \'marketplace\') GROUP BY 1, 2, 3, 4 ), demand AS ( SELECT category, date, cost_price, price, SUM(gross_quantity) OVER ( PARTITION BY category, cost_price ORDER BY date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW ) AS gross_quantity_30 FROM main_data ORDER BY category, cost_price, date ), supply AS ( SELECT date, cost_price::int as cost_price, price::int as price, category::varchar AS category, SUM(ifnull(total_inventory,0)) AS current_inventory FROM snitch_db.maplemonk.cut_size_analysis where date = current_date GROUP BY 1, 2, 3, 4 ) SELECT a.*, coalesce(b.current_inventory,0) as current_inventory FROM demand a LEFT JOIN supply b ON a.date = b.date AND a.category = b.category AND a.cost_price = b.cost_price and a.price = b.price ); ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.shopify_discount_bracket as ( with slashed_sku as ( SELECT upper(trim(VARIANT.value:sku::STRING)) as sku, VARIANT.value:compare_at_price::integer as cp_price, VARIANT.value:price::integer as price, ((cp_price - price)/cp_price)*100 as slashed_discount_percentage FROM snitch_db.maplemonk.shopifyindia_new_products, TABLE(FLATTEN(INPUT => PARSE_JSON(variants))) AS VARIANT where cp_price is not null and cp_price > 0 and price < cp_price ), inventory as ( SELECT upper(trim(sku)) as sku, category::varchar AS category, cost_price::int as cost_price, SUM(ifnull(total_inventory,0)) AS current_inventory FROM snitch_db.maplemonk.cut_size_analysis where date = current_date GROUP BY 1, 2, 3 ) select a.*, coalesce(b.slashed_discount_percentage,0) as slashed_discount_percentage from inventory a left join slashed_sku b on a.sku = b.sku );",
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
            