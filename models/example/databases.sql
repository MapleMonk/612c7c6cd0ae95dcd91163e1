{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.metafields_data as ( WITH master AS ( select sku_group,id,price,product_name,sellable_inventory, case when category in (\'Shirt\', \'Shirts\') then \'Shirts\' when category = \'Denim\' then \'Jeans\' else category end as category from snitch_db.maplemonk.availability_master_v2 ), live_date as ( select a.id, a.status, a.published_at as live_date, b.price, b.sku_group, from snitch_db.maplemonk.shopifyindia_new_products a left join snitch_db.maplemonk.availability_master_v2 b on a.id = b.id ), product_type as ( SELECT id, producttype FROM ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, producttype, ROW_NUMBER() OVER ( PARTITION BY SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) ORDER BY REPLACE(A.value:updated_at, \'\"\', \'\') DESC ) AS rw FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A ) WHERE rw = 1 ), occassion AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS occassion FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'occassion_new\' ), print_design AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS print_design FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'print_design\' ), collar AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS collar FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'collar\' ), material AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS material FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'material\' ), sleeves_type AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS sleeves_type FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'sleeves_type\' ), fit AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS fit FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'fit\' ), color AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(REPLACE(REPLACE(A.value:values, \'[\', \'\'), \']\', \'\'), \'\"\', \'\') AS color FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => options) A WHERE REPLACE(A.value:position, \'\"\', \'\') = 1 ), style AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS style FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'style\' ), closure AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS closure FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'closure\' ), length AS ( SELECT SUBSTRING(id, POSITION(\'Product/\' IN id) + LENGTH(\'Product/\')) AS id, REPLACE(A.value:key, \'\"\', \'\') AS key_type, REPLACE(A.value:value, \'\"\', \'\') AS length FROM snitch_db.maplemonk.metafields_products_graph_ql, LATERAL FLATTEN (input => metafields) A WHERE key_type = \'length\' ), productid as ( select sku_group,id,price,status from (select distinct a.id, REVERSE(SUBSTRING(REVERSE(b.sku),CHARINDEX(\'-\', REVERSE(b.sku)) + 1)) AS SKU_GROUP ,to_timestamp(replace(b.updated_at,\'\"\',\'\')) as UPDATED_AT , row_number ()over (partition by SKU_group order by b.updated_at desc) as rw , b.price as price,status from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS a left join snitch_db.maplemonk.shopifyindia_product_variants b on a.id = b.product_id ) where rw=1 ) SELECT master.*, product_type.producttype as product_type, occassion.occassion AS occassion, print_design.print_design AS print_design, collar.collar AS collar, material.material AS material, sleeves_type.sleeves_type AS sleeve_type, fit.fit AS fit, style.style as style, color.color AS color, closure.closure as closure, length.length as length, productid.id as product_id, case when lower(print_design) like \'%checks%\' then \'Checks\' when lower(print_design) like \'%animal%\' then \'Animal Print\' when lower(print_design) like \'%embroid%\' then \'Embroided\' when lower(print_design) like \'%floral%\' then \'Floral\' when lower(print_design) like \'%graphic%\' then \'Graphic Print\' when lower(print_design) like \'%embellished%\' then \'Embellished\' when lower(print_design) like \'%plain%\' then \'Solid\' when lower(print_design) like \'%solid%\' then \'Solid\' when lower(print_design) like \'%printed%\' then \'Printed\' when lower(print_design) like \'%self%\' then \'Self Design\' when lower(print_design) like \'%stripe%\' then \'Stripes\' when lower(print_design) like \'%crochet%\' then \'Crochet\' when lower(print_design) like \'%polka%\' then \'Polka Dots\' when lower(print_design) like \'%geometric%\' then \'Geometric\' when lower(print_design) like \'%stripe%\' then \'Stripes\' when lower(print_design) like \'%tie &%\' then \'Tie & Dye\' when lower(print_design) like \'%textured%\' then \'Textured\' Else \'Others\' END as Designs, case when lower(collar) like \'%cuban%\' then \'Cuban\' when lower(collar) like \'%crew%\' then \'Round\' when lower(collar) like \'%round%\' then \'Round\' when lower(collar) like \'%polo%\' then \'Polo\' when lower(collar) like \'%high neck%\' then \'High Neck\' when lower(collar) like \'%spread%\' then \'Spread\' when lower(collar) like \'%button down%\' then \'Button Down\' when lower(collar) like \'%mandarin%\' then \'Mandarin\' when lower(collar) like \'%baseball%\' then \'Baseball\' when lower(collar) like \'%classic%\' then \'Classic\' when lower(collar) like \'%shawl%\' then \'Shawl\' when lower(collar) like \'%hooded%\' then \'Hooded\' when lower(collar) like \'%mock%\' then \'Round\' when lower(collar) like \'%turtleneck%\' then \'Turtle Neck\' when lower(collar) like \'%camp%\' then \'Cuban\' Else \'Others\' END as Collar_New, case when lower(material) like \'%cotton%\' then \'Cotton\' when lower(material) like \'%poly%\' then \'Polyester\' when lower(material) like \'%rayon%\' then \'Rayon\' when lower(material) like \'%linen%\' then \'Linen\' when lower(material) like \'%terry%\' then \'Terry\' when lower(material) like \'%Cotton Poly%\' then \'Cotton\' when lower(material) like \'%modal%\' then \'Modal\' when lower(material) like \'%tencel%\' then \'Tencel\' when lower(material) like \'%nylon%\' then \'Nylon\' when lower(material) like \'%tencil%\' then \'Tencil\' when lower(material) like \'%steel%\' then \'Stainless Steel\' Else \'Others\' END as Material_New, case when lower(occassion) like \'%casual%\' then \'Casual Wear\' when lower(occassion) like \'%formal%\' then \'Formal Wear\' when lower(occassion) like \'%club%\' then \'Club Wear\' when lower(occassion) like \'%festive%\' then \'Festive Wear\' when lower(occassion) like \'%elevated%\' then \'Elevated\' when lower(occassion) like \'%intimate%\' then \'Intimate\' when lower(occassion) like \'%sleep%\' then \'Sleep and Lounge Wear\' when lower(occassion) like \'%athletic%\' then \'Athletic\' when lower(occassion) like \'%college%\' then \'College Wear\' when lower(occassion) like \'%street%\' then \'Street Wear\' Else \'Others\' END as occassion_New, case when product_type is null then 0 else 1 end as product_type_fill_rate, case when occassion is null then 0 else 1 end as occassion_fill_rate, case when PRINT_DESIGN is null then 0 else 1 end as PRINT_DESIGN_fill_rate, case when COLLAR is null then 0 else 1 end as COLLAR_fill_rate, case when MATERIAL is null then 0 else 1 end as MATERIAL_fill_rate, case when SLEEVE_TYPE is null then 0 else 1 end as SLEEVE_TYPE_fill_rate, case when FIT is null then 0 else 1 end as FIT_fill_rate, case when style is null then 0 else 1 end as style_fill_rate, case when color is null then 0 else 1 end as color_fill_rate, case when closure is null then 0 else 1 end as closure_fill_rate, case when length is null then 0 else 1 end as length_fill_rate, case when designs is null then 0 else 1 end as designs_fill_rate, case when collar_new is null then 0 else 1 end as collar_new_fill_rate, case when material_new is null then 0 else 1 end as material_new_fill_rate, case when occassion_new is null then 0 else 1 end as occassion_new_fill_rate, FROM master LEFT JOIN product_type ON master.id = product_type.id LEFT JOIN occassion ON master.id = occassion.id LEFT JOIN print_design ON master.id = print_design.id LEFT JOIN collar ON master.id = collar.id LEFT JOIN material ON master.id = material.id LEFT JOIN sleeves_type ON master.id = sleeves_type.id LEFT JOIN fit ON master.id = fit.id LEFT JOIN style on master.id = style.id LEFT JOIN color ON master.id = color.id LEFT JOIN closure ON master.id = closure.id LEFT JOIN length ON master.id = length.id LEFT JOIN productid on master.sku_group = productid.sku_group left join live_date on master.sku_group = live_date.sku_group where lower(live_date.status) = \'active\' ); create or replace table snitch_db.maplemonk.metafields_fill_rate as ( with live_date as ( select a.id, a.status, a.published_at as live_date, b.price, b.sku_group, from snitch_db.maplemonk.shopifyindia_new_products a left join snitch_db.maplemonk.availability_master_v2 b on a.id = b.id ), main_data as ( select sku_group, product_type, sum(sellable_inventory) as inventory, case when product_type in (\'Joggers & Trackpants\',\'Denim\',\'Cargo Pants\',\'Boxers\',\'Night Suit & Pyjamas\',\'Trousers\',\'Chinos\',\'Shorts\',\'Inner Wear\',\'Jeans\') then sum(color_fill_rate+MATERIAL_fill_rate+FIT_fill_rate+occassion_fill_rate+style_fill_rate+product_type_fill_rate+closure_fill_rate+length_fill_rate+PRINT_DESIGN_fill_rate)/9 when product_type in (\'Shirts\',\'Shirt\',\'Kurta\',\'Overshirt\',\'Hoodies\',\'Sweaters\',\'T-Shirts\',\'Co-ords\',\'Sweatshirts\',\'Jackets\',\'Blazers\') then sum(color_fill_rate+MATERIAL_fill_rate+COLLAR_fill_rate+FIT_fill_rate+occassion_fill_rate+style_fill_rate+SLEEVE_TYPE_fill_rate+product_type_fill_rate+PRINT_DESIGN_fill_rate)/9 when product_type in (\'Accessories\',\'Sunglasses\',\'Shoes\',\'Perfumes\') then sum(color_fill_rate+MATERIAL_fill_rate+style_fill_rate+occassion_fill_rate+product_type_fill_rate)/5 end as fill_rate, case when product_type in (\'Joggers & Trackpants\',\'Denim\',\'Cargo Pants\',\'Boxers\',\'Night Suit & Pyjamas\',\'Trousers\',\'Chinos\',\'Shorts\',\'Inner Wear\',\'Jeans\') then sum(color_fill_rate+MATERIAL_fill_rate+FIT_fill_rate+occassion_fill_rate+style_fill_rate+product_type_fill_rate+closure_fill_rate+length_fill_rate+PRINT_DESIGN_fill_rate) when product_type in (\'Shirts\',\'Shirt\',\'Kurta\',\'Overshirt\',\'Hoodies\',\'Sweaters\',\'T-Shirts\',\'Co-ords\',\'Sweatshirts\',\'Jackets\',\'Blazers\') then sum(color_fill_rate+MATERIAL_fill_rate+COLLAR_fill_rate+FIT_fill_rate+occassion_fill_rate+style_fill_rate+SLEEVE_TYPE_fill_rate+product_type_fill_rate+PRINT_DESIGN_fill_rate) when product_type in (\'Accessories\',\'Sunglasses\',\'Shoes\',\'Perfumes\') then sum(color_fill_rate+MATERIAL_fill_rate+occassion_fill_rate+style_fill_rate+product_type_fill_rate) end as filled_metafields, from snitch_db.maplemonk.metafields_data group by 1,2 ) select a.* from main_data a left join live_date b on a.sku_group = b.sku_group where lower(b.status) = \'active\' );",
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
            