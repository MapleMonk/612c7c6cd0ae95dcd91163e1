{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.duplicate_sku_shopify as ( with main as ( SELECT CASE WHEN LEFT(v.value:sku::STRING, 2) = \'4C\' THEN REGEXP_REPLACE( v.value:sku::STRING, \'^(([^-]+-){2}[^-]+)-.*$\', \'\\1\' ) WHEN REGEXP_COUNT(v.value:sku::STRING, \'-\') = 2 THEN REGEXP_REPLACE( v.value:sku::STRING, \'-[^-]+$\', \'\' ) ELSE v.value:sku::STRING END AS sku_group, o.value:values::ARRAY AS size_array FROM snitch_db.maplemonk.new_meafields_product_products_graph_ql AS t, LATERAL FLATTEN(input => PARSE_JSON(t.variants)) AS v, LATERAL FLATTEN(input => PARSE_JSON(t.options)) AS o WHERE o.value:position::INT = 2 ), sizes_num as ( SELECT sku_group, size_array, ARRAY_SIZE(size_array) AS no_of_sizes FROM main qualify row_number() over (partition by sku_group order by ARRAY_SIZE(size_array) desc) = 1 ), skucount as ( select sku_group, count(*) as sku_count from main group by 1 ) select a.*, b.sku_count, sku_count/no_of_sizes as duplicate from sizes_num a left join skucount b on a.sku_group = b.sku_group where sku_count > no_of_sizes and sku_count/no_of_sizes = 2 );",
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
            