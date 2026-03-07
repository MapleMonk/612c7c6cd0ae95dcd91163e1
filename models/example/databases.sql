{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.TUCO_KIDS_FINAL_SKU_MASTER; CREATE TABLE public.TUCO_KIDS_FINAL_SKU_MASTER AS with listing_mast as ( select * from ( select upper(Marketplace) MARKETPLACE ,upper(replace(\"Marketplace SKU\",\'`\',\'\')) MARKETPLACE_SKU ,upper(coalesce(replace(\"Master SKU\",\'`\',\'\'),replace(\"Marketplace SKU\",\'`\',\'\'))) COMMONSKU ,upper(replace(Identifier,\'`\',\'\')) SKU_IDENTIFIER ,upper(replace(Identifier1,\'`\',\'\')) SKU_IDENTIFIER1 ,upper(replace(Identifier2,\'`\',\'\')) SKU_IDENTIFIER2 ,upper(replace(ID,\'`\',\'\')) SKU_ID ,upper(title) Product_name ,row_number() over (partition by upper(marketplace),upper(replace(\"Marketplace SKU\",\'`\',\'\')) order by length(replace(\"Master SKU\",\'`\',\'\')) desc) as rw from public.unbottle_sku_listing_master ) where rw = 1 ), PRODUCT_MASTER AS ( SELECT upper(replace(SKU,\'`\',\'\')) COMMONSKU, Product_id, upper(product_name) PRODUCT_NAME, upper(category_name) CATEGORY, upper(product_type) PRODUCT_TYPE, upper(SIZE) SIZE, Upper(colour) COLOR, cast(cost as double precision) COST, cast(tax_rate as double precision) TAX_RATE, product_image_url, concat(\'<img src=\"\',concat(product_image_url,\'\"width=\"70\">\')) as Image, model_no, cast(active as int) as is_active, (cost::double precision) as cogs, (mrp::double precision) as mrp FROM public.Easyecom_Tuco_Kids_product_master ) select lm.marketplace_sku, lm.marketplace, pm.COMMONSKU, lm.SKU_ID as Marketplace_ID, pm.PRODUCT_ID, pm.PRODUCT_NAME, pm.CATEGORY, pm.product_type, pm.SIZE, pm.COLOR, pm.MRP, pm.COGS, pm.TAX_RATE, pm.image, pm.product_image_url from PRODUCT_MASTER pm left join listing_mast lm on lm.commonsku = pm.commonsku;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            