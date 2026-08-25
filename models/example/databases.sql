{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE DANIEL_KLEIN_DB.MAPLEMONK.Google_Sheet_SKU_MASTER as SELECT UPPER(TRIM(CAST(PRIMARYKEY AS STRING))) AS MASTER_SKU, UPPER(TRIM(CAST(\"DK SKU\" AS STRING))) AS DK_SKU, UPPER(TRIM(CAST(CATEGORY AS STRING))) AS PRODUCT_CATEGORY, UPPER(TRIM(CAST(SUB_CATEGORY AS STRING))) AS PRODUCT_SUB_CATEGORY, Cast(MRP as INT) as MRP, UPPER(TRIM(CAST(\"Catalog name\" AS STRING))) AS CATALOG_NAME, UPPER(TRIM(CAST(\"Bucket 1\" AS STRING))) AS BUCKET_1, UPPER(TRIM(CAST(\"Bucket 2\" AS STRING))) AS BUCKET_2, UPPER(TRIM(CAST(\"MYNTRA Style ID\" AS STRING))) AS MYNTRA_SKU, UPPER(TRIM(CAST(\"Amazon ASIN\" AS STRING))) AS AMAZON_SKU, UPPER(TRIM(CAST(\"Nykaa Fashion\" AS STRING))) AS NYKAA_SKU, UPPER(TRIM(CAST(\"Tatacliq Code\" AS STRING))) AS TATACLIQ_SKU, UPPER(TRIM(CAST(\"Tatacliq Luxury Code\" AS STRING))) AS TATACLIQ_LUXURY_SKU, UPPER(TRIM(CAST(\"Ajio Code\" AS STRING))) AS AJIO_SKU, UPPER(TRIM(CAST(\"SHOPIFY\" AS STRING))) AS SHOPIFY_SKU, UPPER(TRIM(CAST(\"KIOS\" AS STRING))) AS KIOS_SKU, UPPER(TRIM(CAST(\"FLIPKART FSN\" AS STRING))) AS FLIPKART_SKU FROM DANIEL_KLEIN_DB.MAPLEMONK.Daniel_Klein_SKU_MASTER;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DANIEL_KLEIN_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            