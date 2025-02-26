{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ras-wh.maplemonk.ras_sku_master as select upper(productSku) SKU ,upper(modelName) Product_Name ,upper(json_extract_scalar(category,\'$.name\')) category ,upper(json_extract_scalar(brand,\'$.name\')) Brand ,ean_upc EAN ,cast(mrp as float64) mrp ,timestamp(updatedAt) Updated_At from MAPLEMONK.edgistify_get_master_product_list qualify row_number() over (partition by upper(productSku) order by 1) =1 ;",
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
            