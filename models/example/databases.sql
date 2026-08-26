{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ozone_pharma_sku_master as with listing_master as ( select upper(name) MARKETPLACE ,upper(replace(MasterSKU,\'`\',\'\')) COMMONSKU ,upper(replace(Identifier,\'`\',\'\')) SKU_IDENTIFIER ,upper(replace(site_uid,\'`\',\'\')) product_id from`MAPLEMONK.pharma_get_marketplace_listing` where replace(MasterSKU,\'`\',\'\') <> \'\' qualify row_number() over (partition by upper(name),upper(replace(site_uid,\'`\',\'\')) order by length(replace(MasterSKU,\'`\',\'\')) desc) = 1 ), commonskumaster as ( select upper(replace(SKU,\'`\',\'\')) COMMONSKU ,PRODUCT_ID ,upper(product_name) PRODUCT_NAME ,upper(category_name) CATEGORY ,upper(SIZE) SIZE ,Upper(colour) COLOR ,upper(brand) as brand ,cast(mrp as float64) MRP ,cast(cost as float64) COST ,upper(hsn_code) as hsn_code ,cast(tax_rate as float64) TAX_RATE ,cast(created_at as datetime) as created_at ,product_type ,product_image_url ,cast(weight as float64) WEIGHT ,cast(height as float64) HEIGHT ,cast(length as float64) LENGTH FROM maplemonk.easyecom_4thd_easyecom_product_master t qualify row_number() over (partition by upper(replace(SKU,\'`\',\'\')) order by 1 desc) = 1 ) select lm.MARKETPLACE ,lm.product_id ,lm.SKU_IDENTIFIER ,cm.COMMONSKU ,cm.brand ,cm.PRODUCT_NAME ,cm.CATEGORY ,cm.SIZE ,cm.COLOR ,cm.MRP ,cm.COST ,cm.hsn_code ,cm.TAX_RATE ,cm.created_at ,cm.product_type ,cm.product_image_url ,cm.WEIGHT ,cm.HEIGHT ,cm.LENGTH ,CONCAT(\'<img src=\"\',cm.product_image_url, \'\" width=\"70\">\') as Image from commonskumaster cm left join listing_master lm on lm.COMMONSKU = cm.COMMONSKU ;",
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
            