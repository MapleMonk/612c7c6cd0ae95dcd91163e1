{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.MEDMONGERS_FINAL_SKU_MASTER AS WITH SKU_MASTER AS ( SELECT COALESCE(IM.MRP,PL.MRP) AS MRP, PL.\"SKU Code\" AS SKU_CODE, PL.CHANNELNAME, CASE WHEN UPPER(CHANNELNAME) LIKE \'%FLIPKART%\' THEN \'FLIPKART\' WHEN UPPER(CHANNELNAME) LIKE \'%AMAZON%\' THEN \'AMAZON\' WHEN UPPER(CHANNELNAME) LIKE \'%SNAPDEAL%\' THEN \'SNAPDEAL\' WHEN UPPER(CHANNELNAME) LIKE \'%PHARMEASY%\' THEN \'PHARMEASY\' WHEN UPPER(CHANNELNAME) LIKE \'%MEESHO%\' THEN \'MEESHO\' WHEN UPPER(CHANNELNAME) LIKE \'%SHOPCLUES%\' THEN \'SHOPCLUES\' WHEN UPPER(CHANNELNAME) LIKE \'%FIRSTCRY%\' THEN \'FIRSTCRY\' WHEN UPPER(CHANNELNAME) LIKE \'%ONDCCOSTBO%\' THEN \'ONDC\' ELSE CHANNELNAME END AS FINAL_MARKETPLACE, PL.\"Seller SKU on Channel\" as channel_sku, PL.\"Selling Price\" AS SELLING_PRICE, PL.\"Channel Product Id\" AS PRODUCT_ID, COALESCE(IM.NAME,PL.\"Product Name on Channel\") AS PRODUCT_NAME, PL.URL, IM.SIZE, IM.\"Cost Price\" COST, IM.\"Category Code\" CATEGORY_CODE, IM.\"Category Name\" CATEGORY, IM.Brand, ROW_NUMBER() OVER ( PARTITION BY COALESCE(PL.\"SKU Code\", IM.SKU_CODE, PL.\"Seller SKU on Channel\"), CHANNELNAME, PL.\"Channel Product Id\" ORDER BY COALESCE(IM.MRP,PL.MRP) DESC ) AS RN FROM maplemonk.medmongers_get_product_listing PL LEFT JOIN ( SELECT DISTINCT \"Product Code\" AS SKU_CODE, MRP, NAME, SIZE, \"Cost Price\", \"Category Code\", \"Category Name\", upper(Brand) brand, FROM maplemonk.medmongers_get_item_master ) IM ON LOWER(TRIM(PL.\"SKU Code\")) = LOWER(TRIM(IM.SKU_CODE)) WHERE \"Status Code\" = \'LINKED\' ) SELECT * FROM SKU_MASTER WHERE RN = 1 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            