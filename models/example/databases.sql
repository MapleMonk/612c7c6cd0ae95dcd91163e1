{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `Maplemonk.Qurez_Zepto_Fact_Items` AS SELECT CAST(EAN AS STRING) AS EAN, coalesce(sm.mrp,CAST(o.MRP AS FLOAT64)) * CAST(Sales__Qty____Units AS INT64) AS MRP, CAST(o.City AS STRING) AS City, CAST(Date AS TIMESTAMP) AS Date, CAST(SKU_Name AS STRING) AS SKU_Name, CAST(Brand_Name AS STRING) AS Brand_Name, CAST(SKU_Number AS STRING) AS SKU_Number, CAST(SKU_Category AS STRING) AS SKU_Category, CAST(o.Selling_Price AS FLOAT64) AS Selling_Price, CAST(Manufacturer_ID AS STRING) AS Manufacturer_ID, CAST(SKU_Sub_Category AS STRING) AS SKU_Sub_Category, CAST(Manufacturer_Name AS STRING) AS Manufacturer_Name, CAST(Gross_Selling_Value AS FLOAT64) AS Gross_Selling_Value, CAST(Sales__Qty____Units AS INT64) AS Sales_Qty_Units, CAST(Gross_Merchandise_Value AS FLOAT64) AS Gross_Merchandise_Value, coalesce(sm.master_sku,sm.sku_code) as commonsku, sm.product_name as product_name_final FROM `Maplemonk.Qurez_Zepto_sales` o LEFT JOIN ( SELECT * FROM `qurez-wh.maplemonk.Qurez_SKU_MASTER` QUALIFY ROW_NUMBER() OVER ( PARTITION BY UPPER(SKU_CODE) ORDER BY SKU_CODE ) = 1 ) sk ON UPPER(sk.SKU_CODE) = UPPER(o.SKU_Number) left join (select cast(trim(mrp) as float64) as mrp, upper(cast(trim(product_name) as string)) as product_name, upper(cast(trim(sku_code) as string)) as sku_code, upper(cast(trim(master_sku) as string)) as master_sku, upper(cast(trim(channel_sku) as string)) as marketplace_sku, upper(trim(channelname)) as marketplace, upper(trim(product_id)) as marketplace_product_id from qurez-wh.MAPLEMONK.google_sheets_sku_master where upper(trim(channelname)) like \'%ZEPTO%\' qualify row_number() over (partition by upper(cast(trim(channel_sku) as string)) order by mrp desc)=1 ) sm on upper(sm.marketplace_sku) = upper(o.sku_number) ;",
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
            