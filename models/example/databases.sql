{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Sirona_zepto_fact_items AS SELECT CAST(EAN AS STRING) AS EAN, \"ZEPTO\" as PORTAL, CAST(zs.MRP AS FLOAT64) AS MRP, CONCAT(CAST(Date AS STRING),\'_\',City,\'_\',SKU_Number,\'_\',Manufacturer_ID) AS order_id, CAST(City AS STRING) AS City, CAST(Date AS TIMESTAMP) AS Date, COALESCE(CAST(mp.Name AS STRING),CAST(zs.SKU_Name as string)) AS SKU_Name, CAST(Brand_Name AS STRING) AS Brand_Name, CAST(SKU_Number AS STRING) AS SKU_Number, COALESCE(CAST(mp.Type as string),CAST(zs.SKU_Category AS STRING)) AS SKU_Category, CAST(zs.MRP AS FLOAT64) * CAST(Sales__Qty____Units AS INT64) AS Selling_Price, CAST(Sales__Qty____Units AS INT64) AS Quantity, CAST(Manufacturer_ID AS STRING) AS Manufacturer_ID, COALESCE(CAST(mp.Sub_Category as string),CAST(SKU_Sub_Category AS STRING)) AS SKU_Sub_Category, CAST(Manufacturer_Name AS STRING) AS Manufacturer_Name, CAST(Gross_Selling_Value AS FLOAT64) AS Gross_Selling_Value, CAST(Sales__Qty____Units AS INT64) AS Sales_Qty_Units, CAST(Gross_Merchandise_Value AS FLOAT64) AS Gross_Merchandise_Value, mp.sirona_sku_code SKU, cast(mp.mrp as float64) * CAST(Sales__Qty____Units AS INT64) as mrp_sales FROM `MAPLEMONK.Sirona_Zepto_db_sales` zs LEFT JOIN ( select MRP, GST_, Name, Type, Portal, Portal_Code, Sub_Category, Sirona_SKU_Code from maplemonk.sirona_db_google_sheet_MP_Master qualify row_number() over(partition by portal, Portal_Code,Sirona_SKU_Code order by 1)=1 ) mp on UPPER(zs.SKU_Number) = UPPER(mp.portal_code) AND UPPER(mp.Portal) = \'ZEPTO\'",
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
            