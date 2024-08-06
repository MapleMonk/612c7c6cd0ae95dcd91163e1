{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.Product_info as WITH master AS ( select *, case when category in (\'Shirt\', \'Shirts\') then \'Shirts\' when category = \'Denim\' then \'Jeans\' else category end as new_category from snitch_db.maplemonk.availability_master_v2 ), product_type as ( SELECT id, producttype FROM ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, producttype, ROW_NUMBER() OVER ( PARTITION BY SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) ORDER BY REPLACE(A.value:updated_at, \'\"\', \'\') DESC ) AS rw FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A ) WHERE rw = 1 ), occassion AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS occassion FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'occassion_new\' ), print_design AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS print_design FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'print_design\' ), collar AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS collar FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'collar\' ), material AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS material FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'material\' ), sleeves_type AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS sleeves_type FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'sleeves_type\' ), fit AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS fit FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'fit\' ), color AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(REPLACE(REPLACE(A.value:values, \'[\', \'\'), \']\', \'\'), \'\"\', \'\') AS color FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => options) A WHERE REPLACE(A.value:position, \'\"\', \'\') = 1 ), style AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS style FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'style\' ), sales as ( SELECT DATE as date_wise, new_category, sku_group, total_sales, total_quant FROM snitch_db.maplemonk.category_visibility ), productid as (SELECT id, sku_group FROM ( SELECT *, REVERSE(SUBSTRING(REVERSE(REPLACE(A.value:sku, \'\"\"\', \'\')), CHARINDEX(\'-\', REVERSE(REPLACE(A.value:sku, \'\"\"\', \'\'))) + 1)) AS sku_group, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY updated_at DESC) AS rw FROM snitch_db.maplemonk.shopify_all_products, LATERAL FLATTEN (input => variants) A ) WHERE rw = 1) SELECT master.*, date_wise, total_sales, total_quant, product_type.producttype as product_type, occassion.occassion AS occassion, print_design.print_design AS print_design, collar.collar AS collar, material.material AS material, sleeves_type.sleeves_type AS sleeve_type, fit.fit AS fit, style.style as style, color.color AS color, productid.id as product_id, case when lower(print_design) like \'%checks%\' then \'Checks\' when lower(print_design) like \'%animal%\' then \'Animal Print\' when lower(print_design) like \'%embroid%\' then \'Embroided\' when lower(print_design) like \'%floral%\' then \'Floral\' when lower(print_design) like \'%graphic%\' then \'Graphic Print\' when lower(print_design) like \'%embellished%\' then \'Embellished\' when lower(print_design) like \'%plain%\' then \'Solid\' when lower(print_design) like \'%solid%\' then \'Solid\' when lower(print_design) like \'%printed%\' then \'Printed\' when lower(print_design) like \'%self%\' then \'Self Design\' when lower(print_design) like \'%stripe%\' then \'Stripes\' when lower(print_design) like \'%crochet%\' then \'Crochet\' when lower(print_design) like \'%polka%\' then \'Polka Dots\' when lower(print_design) like \'%geometric%\' then \'Geometric\' when lower(print_design) like \'%stripe%\' then \'Stripes\' when lower(print_design) like \'%tie &%\' then \'Tie & Dye\' when lower(print_design) like \'%textured%\' then \'Textured\' when lower(print_design) IS NULL then \'Null\' Else \'Others\' END as Designs, case when lower(collar) like \'%cuban%\' then \'Cuban\' when lower(collar) like \'%crew%\' then \'Round\' when lower(collar) like \'%round%\' then \'Round\' when lower(collar) like \'%polo%\' then \'Polo\' when lower(collar) like \'%high neck%\' then \'High Neck\' when lower(collar) like \'%spread%\' then \'Spread\' when lower(collar) like \'%button down%\' then \'Button Down\' when lower(collar) like \'%mandarin%\' then \'Mandarin\' when lower(collar) like \'%baseball%\' then \'Baseball\' when lower(collar) like \'%classic%\' then \'Classic\' when lower(collar) like \'%shawl%\' then \'Shawl\' when lower(collar) like \'%hooded%\' then \'Hooded\' when lower(collar) like \'%mock%\' then \'Round\' when lower(collar) like \'%turtleneck%\' then \'Turtle Neck\' when lower(collar) like \'%camp%\' then \'Cuban\' when lower(collar) IS NULL then \'Null\' Else \'Others\' END as Collar_New, case when lower(material) like \'%cotton%\' then \'Cotton\' when lower(material) like \'%poly%\' then \'Polyester\' when lower(material) like \'%rayon%\' then \'Rayon\' when lower(material) like \'%linen%\' then \'Linen\' when lower(material) like \'%terry%\' then \'Terry\' when lower(material) like \'%Cotton Poly%\' then \'Cotton\' when lower(material) like \'%modal%\' then \'Modal\' when lower(material) like \'%tencel%\' then \'Tencel\' when lower(material) like \'%nylon%\' then \'Nylon\' when lower(material) like \'%tencil%\' then \'Tencil\' when lower(material) like \'%steel%\' then \'Stainless Steel\' when lower(material) IS NULL then \'Null\' Else \'Others\' END as Material_New, case when lower(occassion) like \'%casual%\' then \'Casual Wear\' when lower(occassion) like \'%formal%\' then \'Formal Wear\' when lower(occassion) like \'%club%\' then \'Club Wear\' when lower(occassion) like \'%festive%\' then \'Festive Wear\' when lower(occassion) like \'%elevated%\' then \'Elevated\' when lower(occassion) like \'%intimate%\' then \'Intimate\' when lower(occassion) like \'%sleep%\' then \'Sleep and Lounge Wear\' when lower(occassion) like \'%athletic%\' then \'Athletic\' when lower(occassion) like \'%college%\' then \'College Wear\' when lower(occassion) like \'%street%\' then \'Street Wear\' when lower(occassion) IS NULL then \'Null\' Else \'Others\' END as occassion_New FROM master LEFT JOIN product_type ON master.id = product_type.id LEFT JOIN occassion ON master.id = occassion.id LEFT JOIN print_design ON master.id = print_design.id LEFT JOIN collar ON master.id = collar.id LEFT JOIN material ON master.id = material.id LEFT JOIN sleeves_type ON master.id = sleeves_type.id LEFT JOIN fit ON master.id = fit.id LEFT JOIN style on master.id = style.id LEFT JOIN color ON master.id = color.id LEFT JOIN sales on master.sku_group = sales.sku_group LEFT JOIN productid on master.sku_group = productid.sku_group",
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
            