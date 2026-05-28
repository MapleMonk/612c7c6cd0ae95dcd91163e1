{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE or replace table snitch_db.maplemonk.core_tshirts_availability as WITH base AS ( SELECT DISTINCT LOWER(SKU_GROUP) AS sku_group_key, SKU_GROUP, original_price, CAT_SKU_CLASS, FINAL_SKU_CLASS, CASE WHEN LOWER(collar) IN (\'crew\', \'round\') THEN \'Round\' WHEN LOWER(collar) = \'polo\' THEN \'Polo\' END AS collar_type, CASE WHEN LOWER(FINAL_SKU_CLASS) LIKE \'cut%\' THEN \'Cut\' ELSE \'Non-Cut\' END AS \"Cut Flag\" FROM snitch_db.maplemonk.base_product WHERE category = \'T-Shirts\' AND LOWER(SKU_GROUP) NOT LIKE \'4mss%\' AND ( (collar IN (\'Crew\', \'Round\') AND original_price = \'599\') OR (collar = \'Polo\' AND original_price = \'999\') ) ), availability AS ( SELECT LOWER(SKU_GROUP) AS sku_group_key, SUM(Available_Units) AS Available_Units FROM snitch_db.maplemonk.availability_master_v2_merged GROUP BY LOWER(SKU_GROUP) ), images AS ( SELECT LOWER(REGEXP_REPLACE(variant.value:sku::STRING, \'-[^-]+$\', \'\')) AS sku_group_key, image.value:preview:image:url::STRING AS image_url, ROW_NUMBER() OVER ( PARTITION BY LOWER(REGEXP_REPLACE(variant.value:sku::STRING, \'-[^-]+$\', \'\')) ORDER BY image.index ) AS rn FROM snitch_db.maplemonk.new_meafields_product_products_graph_ql t, LATERAL FLATTEN(input => PARSE_JSON(t.media)) AS image, LATERAL FLATTEN(input => PARSE_JSON(t.variants)) AS variant WHERE image.value:mediaContentType::STRING = \'IMAGE\' QUALIFY rn = 1 ) SELECT o.*, a.Available_Units, b.original_price, b.CAT_SKU_CLASS, b.FINAL_SKU_CLASS, b.collar_type, b.\"Cut Flag\", REGEXP_REPLACE(LOWER(o.SKU_GROUP), \'-[^-]+$\', \'\') AS \"SKU Family\", i.image_url FROM snitch_db.maplemonk.offline_master o INNER JOIN base b ON LOWER(o.SKU_GROUP) = b.sku_group_key LEFT JOIN availability a ON LOWER(o.SKU_GROUP) = a.sku_group_key LEFT JOIN images i ON LOWER(o.SKU_GROUP) = i.sku_group_key WHERE LOWER(o.SKU_GROUP) NOT LIKE \'4mss%\';",
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
            