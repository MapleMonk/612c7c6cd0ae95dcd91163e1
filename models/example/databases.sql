{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MAPLEMONK.RAR_SKU_MASTER` AS WITH SKU_MASTER AS ( SELECT CASE WHEN COALESCE(IM.MRP, PL.MRP) = \'\' THEN NULL ELSE COALESCE(IM.MRP, PL.MRP) END AS MRP, PL.SKU_Code AS SKU_CODE, PL.CHANNELNAME, CASE WHEN UPPER(CHANNELNAME) LIKE \'%FLIPKART%\' THEN \'FLIPKART\' WHEN UPPER(CHANNELNAME) LIKE \'%AMAZON%\' THEN \'AMAZON\' WHEN UPPER(CHANNELNAME) LIKE \'%SNAPDEAL%\' THEN \'SNAPDEAL\' WHEN UPPER(CHANNELNAME) LIKE \'%PHARMEASY%\' THEN \'PHARMEASY\' WHEN UPPER(CHANNELNAME) LIKE \'%MEESHO%\' THEN \'MEESHO\' WHEN UPPER(CHANNELNAME) LIKE \'%SHOPCLUES%\' THEN \'SHOPCLUES\' WHEN UPPER(CHANNELNAME) LIKE \'%FIRSTCRY%\' THEN \'FIRSTCRY\' WHEN UPPER(CHANNELNAME) LIKE \'%ONDCCOSTBO%\' THEN \'ONDC\' ELSE CHANNELNAME END AS FINAL_MARKETPLACE, PL.Seller_SKU_on_Channel AS channel_sku, PL.Selling_Price AS SELLING_PRICE, PL.Channel_Product_Id AS PRODUCT_ID, COALESCE( PL.Product_Name_on_Channel, IM.NAME ) AS PRODUCT_NAME, coalesce(im.image_url,image.url,nullif(trim(PL.URL),\'\')) as url, IM.SIZE, CASE WHEN IM.Cost_Price = \'\' THEN NULL ELSE CAST(IM.Cost_Price AS FLOAT64) END AS COST, IM.Category_Code AS CATEGORY_CODE, IM.Category_Name AS CATEGORY, CASE WHEN UPPER(IM.brand) LIKE \'%IN YOU%\' OR UPPER(IM.brand) LIKE \'%INYOU%\' THEN \'IN YOU\' ELSE UPPER(IM.brand) END AS Brand, ROW_NUMBER() OVER ( PARTITION BY COALESCE( PL.SKU_Code, IM.SKU_CODE, PL.Seller_SKU_on_Channel ), CHANNELNAME, PL.Channel_Product_Id ORDER BY COALESCE(IM.MRP, PL.MRP) DESC ) AS RN FROM `MAPLEMONK.RAR_Unicommerce_get_product_listing` PL LEFT JOIN ( SELECT Product_Code AS SKU_CODE, MRP, NAME, SIZE, Cost_Price, Category_Code, Category_Name, UPPER(Brand) AS brand, nullif(trim(image_url),\'\') as image_url FROM `MAPLEMONK.RAR_Unicommerce_get_product_master` qualify row_number() over (partition by upper(product_code) order by 1 desc)=1 ) IM ON LOWER(TRIM(PL.SKU_Code)) = LOWER(TRIM(IM.SKU_CODE)) left join (select upper(sku_code) as sku, nullif(trim(url),\'\') as url FROM `MAPLEMONK.RAR_Unicommerce_get_product_listing` qualify row_number() over (partition by upper(sku_code) order by nullif(trim(url),\'\') desc)=1) image on lower(image.sku) = LOWER(TRIM(PL.SKU_Code)) WHERE PL.Status_Code = \'LINKED\' ) SELECT * FROM SKU_MASTER WHERE RN = 1;",
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
            