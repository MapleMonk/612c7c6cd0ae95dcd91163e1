{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.final_latest_SKU_MASTER as select upper(cast(trim(ASIN__Amazon_) as string)) as amazon_sku, upper(cast(trim(ASIN__Amazon_) as string)) as amazon_vc_sku, upper(cast(trim(Serial_Number__Flipkart_) as string)) as flipkart_sku, upper(cast(trim(BlinkIT_SKU) as string)) as blinkit_sku, upper(cast(trim(Instamart_SKU) as string)) as swiggy_sku, upper(cast(trim(Master_SKU) as string)) as master_sku, upper(cast(trim(Old_Sku_Name) as string)) as Old_Sku_Name, upper(cast(trim(ProductName) as string)) as product_name, upper(cast(trim(Category) as string)) as category, upper(cast(trim(Brand) as string)) as brand, upper(cast(trim(Product) as string)) as product, upper(cast(trim(Pack_Type) as string)) as pack_type, upper(cast(trim(Pack_Variant) as string)) as pack_variant, upper(cast(trim(SKUType) as string)) as sku_type, upper(cast(trim(Material_Type) as string)) as material_type, upper(cast(trim(ImageURL) as string)) as image_url, upper(cast(trim(Size) as string)) as size, cast(trim(mrp) as float64) as mrp, cast(trim(cost) as float64) as cost, cast(trim(status) as string) as status, upper(trim(flavour)) as flavour, upper(trim(Cat)) as Cat, upper(trim(Subcat_1)) as Subcat_1, upper(trim(Subcat_2)) as Subcat_2, cast(trim(EAN_UPC) as string) as EAN_UPC, from maplemonk.Prolicious_gs_final_SKU_MASTER g qualify row_number() over (partition by upper(cast(trim(Master_SKU) as string)),upper(cast(trim(ASIN__Amazon_) as string)) order by length(replace(pack_type,\'`\',\'\')) desc) = 1 ;",
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
            