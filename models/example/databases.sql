{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.Facebook_Catalog AS with availability_master as ( select * FROM snitch_db.maplemonk.availability_master_v2 ), description_merge as ( SELECT distinct legacyresourceid, Description, onlinestoreurl FROM Snitch_db.MAPLEMONK.SHOPIFYINDIA_PRODUCTS_GRAPH_QL ), image_merge as ( SELECT distinct id, replace(A.value:src,\'\"\',\'\') as src, replace(A.value:position,\'\"\',\'\') as pst FROM snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS, LATERAL FLATTEN(INPUT => IMAGES)A where pst = 1 ) select S_UNITS as S, M_UNITS as M, L_UNITS as L, product_id as id, product_name as title, description, price, src as image_link, onlinestoreurl as link, sku_group, category, FINAL_ROS, available_units, sku_class FROM availability_master LEFT JOIN description_merge ON availability_master.product_id = description_merge.legacyresourceid LEFT JOIN image_merge ON description_merge.legacyresourceid = image_merge.id",
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
                        