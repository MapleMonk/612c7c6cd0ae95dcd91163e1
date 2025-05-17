{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.correct_price as with price as ( select distinct LEFT(value:sku::STRING, LENGTH(value:sku::STRING) - POSITION(\'-\' in REVERSE(value:sku::STRING))) as sku_group, split_part(id,\'/\',-1) as id, ifnull(value:compareAtPrice::NUMBER,0)::int as compare_at_price, value:price::NUMBER::int as price from snitch_db.maplemonk.new_meafields_product_products_graph_ql, lateral flatten(input => parse_json(variants)) as value ), final_price1 as ( select sku_group, id, greatest(compare_at_price,price) AS db_original_price, CASE WHEN compare_at_price != 0 THEN LEAST(compare_at_price,price) ELSE GREATEST(price,compare_at_price) END AS db_slashed_price from price ) select sku_group, id, db_original_price as original_price, db_slashed_price as current_slashed_price, from final_price1 where sku_group not in (\'\',\'1290\') qualify row_number() over (partition by sku_group order by db_original_price desc) = 1 ;",
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
            