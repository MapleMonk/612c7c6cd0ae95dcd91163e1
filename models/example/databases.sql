{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.FINAL_SKU_MASTER AS With Marketplace_SKU as ( select upper(trim(Marketplace)) Marketplace ,upper(trim(Marketplace_SKU)) Marketplace_SKU ,upper(trim(Marketplace_SKU)) Channel_Product_Id ,upper(trim(COMMONSKU)) as COMMONSKU ,upper(source) Data_source from maplemonk.ka_ideal_sku_mapping qualify row_number() over(partition by upper(trim(ifnull(source,\'\'))), upper(trim(ifnull(Marketplace,\'\'))),upper(trim(ifnull(Marketplace_SKU,\'\'))) order by upper(trim(ifnull(COMMONSKU,\'\'))) desc ) = 1 ), CommonSKU_Master as ( SELECT UPPER(TRIM(COMMONSKU)) AS commonsku_master, UPPER(TRIM(Product_Name)) AS Name, cast (NULL as string) AS colour, cast (NULL as string) AS Brand, UPPER(TRIM(`Group`)) AS Category, upper (Category) AS Category_Code, safe_CAST(REPLACE(MRP, \',\', \'\') AS FLOAT64) AS MRP, SAFE_CAST(REPLACE(GST, \',\', \'\') AS FLOAT64) AS TAX_RATE, CAST(REPLACE(Gross_Weight_Including_packaging_weight, \',\', \'\') AS FLOAT64) AS weight, SAFE_CAST(REPLACE(length, \',\', \'\') AS FLOAT64) AS length, SAFE_CAST(REPLACE(Height, \',\', \'\') AS FLOAT64) AS Height, SAFE_CAST(REPLACE(Breadth_Dia, \',\', \'\') AS FLOAT64) AS Width, cast (NULL as string) AS volume, CAST(REPLACE(Gross_Weight_Including_packaging_weight, \',\', \'\') AS FLOAT64) AS Packing_Weight, CAST(REPLACE(Gross_Weight_Including_packaging_weight, \',\', \'\') AS FLOAT64) AS Weight_Incl_Packing, HSN AS HSN_CODE, UOM, `Case`, Prime, Alt_UOM, Packing, EAN_Code, Grammage FROM maplemonk.KA_SKU_MASTER qualify row_number() over(partition by UPPER(TRIM(COMMONSKU)) order by UPPER(TRIM(ifnull(COMMONSKU,\'\'))) desc ) = 1 ) select coalesce(l.COMMONSKU,cm.commonsku_master) as COMMONSKU ,l.MARKETPLACE_SKU ,l.MARKETPLACE ,l.Channel_Product_Id ,l.Data_source ,cm.* from Marketplace_SKU l Full Outer join CommonSKU_Master CM on upper(l.commonsku) = upper(cm.commonsku_master);",
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
            