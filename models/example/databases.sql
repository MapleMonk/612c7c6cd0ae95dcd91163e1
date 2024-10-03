{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.supplemtary_feed as ( SELECT sku_group AS \"id\", CASE WHEN sku_group LIKE \'4MBG%\' THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', \'Plus Size\', \' | \', \'3XL, 4XL, 5XL, 6XL\', \' | \', material, \' | \', collar, \' | \', print_design, \' | \', sleeve_type, \' | \', occassion) WHEN category = \'Blazers\' THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', material, \' | \', fit, \' | \', collar, \' | \', print_design, \' | \', sleeve_type, \' | \', occassion) WHEN category IN (\'Boxers\', \'Night Suit & Pyjamas\') THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', material, \' | \', print_design, \' | \', occassion, \' | \', fit, \' | \', closure, \' | \', length) WHEN category IN (\'Cargo Pants\', \'Chinos\', \'Joggers & Trackpants\', \'Shorts\', \'Trousers\') THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', fit, \' | \', material, \' | \', print_design, \' | \', closure, \' | \', length, \' | \', occassion) WHEN category = \'Co-ords\' THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', material, \' | \', fit, \' | \', print_design, \' | \', sleeve_type, \' | \', collar, \' | \', length, \' | \', closure, \' | \', occassion) WHEN category = \'Denim\' THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', fit, \' | \', length, \' | \', material, \' | \', print_design, \' | \', closure, \' | \', occassion) WHEN category IN (\'Hoodies\', \'Jackets\', \'Sweaters\', \'Sweatshirts\', \'T-shirts\') THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', fit, \' | \', material, \' | \', print_design, \' | \', collar, \' | \', sleeve_type, \' | \', occassion) WHEN category IN (\'Kurta\', \'Overshirt\', \'Shirt\', \'Shirts\') THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', fit, \' | \', material, \' | \', collar, \' | \', print_design, \' | \', occassion, \' | \', sleeve_type) WHEN TRIM(category) IN (\'T-shirts\', \'T-Shirts\') THEN CONCAT(\'Snitch Men\'\'s \', title, \' | \', fit, \' | \', material, \' | \', print_design, \' | \', collar, \' | \', sleeve_type, \' | \', occassion) END AS title, description AS \"description\", \'in stock\' AS \"availability\", link AS \"link\", image_link AS \"image_link\", price AS \"price\", \'SNITCH\' as \"brand\", --additional image link, \'new\' AS \"condition\", \'yes\' as \"adult\", color AS \"color\", case when sku_group like \'4MBG%\' then \'3XL-6XL\' else \'S-L\' end as \"size\", \'male\' as \"gender\", material AS \"material\", print_design AS \"pattern\" FROM snitch_db.maplemonk.facebook_catalog WHERE title IS NOT NULL GROUP BY sku_group, title, description, price, link, image_link, sku_class, category, S, M, L, occassion, print_design, collar, material, sleeve_type, fit, color, designs, closure, style, length, XL3_UNITS, XL4_UNITS, XL5_UNITS, XL6_UNITS HAVING (SUM(available_units) > 45 AND S > 10 AND M > 10 AND L > 10) OR (LOWER(sku_group) LIKE \'4mbg%\' AND SUM(available_units) > 45) );",
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
            