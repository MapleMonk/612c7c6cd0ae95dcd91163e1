{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_ZEPTO_FACT_ITEMS AS SELECT EAN, CONCAT(EAN, \"SKU Number\", CITY, DATE) AS ORDER_ITEM, CAST(MRP AS FLOAT) AS MRP, CITY, CAST(DATE AS TIMESTAMP) AS ORDER_DATE, \"SKU Name\" AS PRODUCT_NAME, \"Brand Name\" AS BRAND, \"SKU Number\" SKU_NUMBER, \"SKU Category\" SKU_CATEGORY, CAST(\"Gross Merchandise Value\" AS FLOAT) AS SELLING_PRICE, \"Manufacturer ID\" MANUFACTURER_ID, \"SKU Sub Category\" SKU_SUB_CATEGORY, CAST(\"Gross Merchandise Value\" AS FLOAT) AS GROSS_SELLING_VALUE, CAST(\"Sales (Qty) - Units\" AS INTEGER) AS QUANTITY, CAST(\"Gross Merchandise Value\" AS FLOAT) AS GROSS_MERCHANDISE_VALUE FROM MAPLEMONK.ZEPTO_PHILIPS_SALES_SALES;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from philips_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            