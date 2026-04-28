{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_FLIPKART_VENDOR_HUB_SALES_FACTiTEMS AS SELECT CAST(EAN AS VARCHAR) AS EAN, CAST(FSN AS VARCHAR) AS FSN, CAST(HSN AS VARCHAR) AS HSN, CAST(ISBN AS VARCHAR) AS ISBN, TO_DATE(DATE) AS ORDER_DATE, CAST(BRAND AS VARCHAR) AS BRAND, TRY_TO_NUMBER(SALES) AS SALES_QTY, CAST(CATEGORY AS VARCHAR) AS CATEGORY, CAST(\"Model ID\" AS VARCHAR) AS MODEL_ID, CAST(VERTICAL AS VARCHAR) AS VERTICAL, CAST(\"Tenant ID\" AS VARCHAR) AS TENANT_ID, CAST(PUBLISHER AS VARCHAR) AS PUBLISHER, CAST(\"Style Code\" AS VARCHAR) AS STYLE_CODE, CAST(\"Retailer ID\" AS VARCHAR) AS RETAILER_ID, CAST(\"Product Title\" AS VARCHAR) AS PRODUCT_TITLE, CAST(\"Retailer Name\" AS VARCHAR) AS RETAILER_NAME, FROM MAPLEMONK.PHILIPS_FLIPKART_VENDOR_HUB_SALES_REPORT;",
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
            