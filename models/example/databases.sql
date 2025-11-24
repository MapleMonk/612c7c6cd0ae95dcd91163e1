{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Gifting_Inventory_Report AS WITH SKU_MASTER AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY Product_Code, Component_Product_Code ORDER BY Updated DESC) AS rn FROM Maplemonk.Zouk_uc_get_product_master ), FILTERED_SKUS AS ( SELECT UPPER(Product_Code) AS Parent_SKU, UPPER(Component_Product_Code) AS Child_SKU, UPPER(Name) AS Product_Name, UPPER(Color) AS Print, UPPER(Collection) AS Collection, UPPER(Product_Type) AS Product_Type, UPPER(Category_Code) AS Category_Code, UPPER(Category_Name) AS Category_Name FROM SKU_MASTER WHERE rn = 1 AND lower(collection) like \'gifting%\' ), INVENTORY AS ( SELECT UPPER(SKU) AS SKU_ID, SUM(Available_Inventory) AS Inventory_Qty, SUM(IFNULL(SOLD_QUANTITY_30_DAYS, 0)) AS Sold_30_Days, FROM Maplemonk.ZOUK_INVENTORY_FACT_ITEMS WHERE DATA_FETCH_DATE = (SELECT MAX(DATA_FETCH_DATE) FROM Maplemonk.ZOUK_INVENTORY_FACT_ITEMS) GROUP BY SKU ) , CHILD_INVENTORY AS ( SELECT f.*, COALESCE(i.Inventory_Qty, 0) AS Child_Inventory, COALESCE(i.SOLD_30_DAYS, 0) AS Sold_30_Days, FROM FILTERED_SKUS f LEFT JOIN INVENTORY i ON f.Child_SKU = i.SKU_ID ), PARENT_COMBO AS ( SELECT Parent_SKU, MIN(c.Child_Inventory) AS Parent_SKU_Inventory FROM CHILD_INVENTORY c GROUP BY Parent_SKU ), RANKED_COMBO AS ( SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.Parent_SKU ORDER BY c.Child_SKU) AS rn, p.Parent_SKU_Inventory FROM CHILD_INVENTORY c LEFT JOIN PARENT_COMBO p ON c.Parent_SKU = p.Parent_SKU ), CHILD_SKU_NAMES AS ( SELECT UPPER(Product_Code) AS Child_SKU, UPPER(Name) AS Child_SKU_Name, ROW_NUMBER() OVER (PARTITION BY Product_Code ORDER BY Updated DESC) AS rn FROM Maplemonk.Unicommerce_zouk_get_product_master ) SELECT Parent_SKU, rc.Child_SKU, csn.Child_SKU_Name, Product_Name, Print, Collection, Product_Type, Category_Code, Category_Name, Child_Inventory, Sold_30_Days, CASE WHEN rc.rn = 1 THEN Parent_SKU_Inventory ELSE 0 END AS Parent_SKU_Inventory FROM RANKED_COMBO rc LEFT JOIN CHILD_SKU_NAMES csn ON rc.Child_SKU = csn.Child_SKU AND csn.rn = 1 ORDER BY Parent_SKU, Child_SKU ;",
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
            