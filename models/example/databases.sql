{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.Facebook_Catalog AS WITH availability_master AS ( SELECT * FROM snitch_db.maplemonk.availability_master_v2 ), description_merge AS ( SELECT DISTINCT legacyresourceid, Description, onlinestoreurl FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_PRODUCTS_GRAPH_QL ), image_merge AS ( SELECT DISTINCT id, REPLACE(A.value:src,\'\"\',\'\') AS src, REPLACE(A.value:position,\'\"\',\'\') AS pst FROM snitch_db.maplemonk.SHOPIFY_ALL_PRODUCTS, LATERAL FLATTEN(INPUT => IMAGES) A WHERE pst = 1 ), metafields AS ( SELECT id, occassion, print_design, collar, material, sleeve_type, fit, color, designs, closure, style, length FROM snitch_db.maplemonk.product_info ) SELECT S_UNITS AS S, M_UNITS AS M, L_UNITS AS L, XL3_UNITS AS XL3_UNITS, XL4_UNITS AS XL4_UNITS, XL5_UNITS AS XL5_UNITS, XL6_UNITS AS XL6_UNITS, availability_master.id as id, product_name AS title, description, price, src AS image_link, onlinestoreurl AS link, sku_group, category, FINAL_ROS, available_units, sku_class, occassion, print_design, collar, material, sleeve_type, fit, color, designs, closure, style, length FROM availability_master LEFT JOIN description_merge ON availability_master.id = description_merge.legacyresourceid LEFT JOIN image_merge ON availability_master.id = image_merge.id LEFT JOIN metafields ON availability_master.id = metafields.id",
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
            