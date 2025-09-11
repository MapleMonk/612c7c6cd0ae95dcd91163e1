{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or Replace table trase-wh.maplemonk.Final_SKU_master as With CTE as ( select trim(amazon_sku) as marketplace_SKU ,trim(amazon_parent_asin) as Marketplace_ID ,trim(new_sku) as new_sku ,trim(color_sku) as color_sku ,trim(parent_sku) as parent_sku ,cast(null as string) as Product_Name ,cast(null as string) as Category ,mrp ,link as image_url ,\'Amazon\' as Marketplace from trase-wh.maplemonk.trase_final_sku_master qualify row_number() over(partition by trim(amazon_sku) order by 1) = 1 UNION ALL select trim(fk_sku) as marketplace_SKU ,trim(fk_listing_id) as Marketplace_ID ,trim(new_sku) as new_sku ,trim(color_sku) as color_sku ,trim(parent_sku) as parent_sku ,cast(null as string) as Product_Name ,cast(null as string) as Category ,mrp ,link as image_url ,\'Flipkart\' as Marketplace from trase-wh.maplemonk.trase_final_sku_master qualify row_number() over(partition by trim(fk_sku) order by 1) = 1 UNION ALL select trim(ajio_sku) as marketplace_SKU ,trim(ajio_Jiocode) as Marketplace_ID ,trim(new_sku) as new_sku ,trim(color_sku) as color_sku ,trim(parent_sku) as parent_sku ,cast(null as string) as Product_Name ,cast(null as string) as Category ,mrp ,link as image_url ,\'Ajio\' as Marketplace from trase-wh.maplemonk.trase_final_sku_master qualify row_number() over(partition by trim(ajio_sku) order by 1) = 1 UNION ALL select trim(myntra_style_id) as marketplace_SKU ,trim(myntra_sku_id) as Marketplace_ID ,trim(new_sku) as new_sku ,trim(color_sku) as color_sku ,trim(parent_sku) as parent_sku ,cast(null as string) as Product_Name ,cast(null as string) as Category ,mrp ,link as image_url ,\'Myntra\' as Marketplace from trase-wh.maplemonk.trase_final_sku_master qualify row_number() over(partition by trim(myntra_sku) order by 1) = 1 UNION ALL select trim(meesho_catalog_id) as marketplace_SKU ,trim(meesho_catalog_id) as Marketplace_ID ,trim(new_sku) as new_sku ,trim(color_sku) as color_sku ,trim(parent_sku) as parent_sku ,cast(null as string) as Product_Name ,cast(null as string) as Category ,mrp ,link as image_url ,\'Meesho\' as Marketplace from trase-wh.maplemonk.trase_final_sku_master qualify row_number() over(partition by trim(new_sku),trim(meesho_catalog_id) order by 1) = 1 ) Select * from CTE qualify row_number() over(partition by lower(ifnull(trim(new_sku),\'\')), marketplace order by 1) = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            