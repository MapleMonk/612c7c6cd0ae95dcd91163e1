{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_FLIPKART_VENDOR_HUB_SALES_FACTITEMS AS SELECT CAST(EAN AS VARCHAR) AS EAN, CAST(FSN AS VARCHAR) AS FSN, CAST(HSN AS VARCHAR) AS HSN, CAST(ISBN AS VARCHAR) AS ISBN, TO_DATE(DATE) AS ORDER_DATE, CAST(BRAND AS VARCHAR) AS BRAND, TRY_TO_NUMBER(SALES) AS SALES_QTY, CAST(\"Model ID\" AS VARCHAR) AS MODEL_ID, CAST(VERTICAL AS VARCHAR) AS VERTICAL, CAST(\"Tenant ID\" AS VARCHAR) AS TENANT_ID, CAST(PUBLISHER AS VARCHAR) AS PUBLISHER, CAST(\"Style Code\" AS VARCHAR) AS STYLE_CODE, CAST(\"Retailer ID\" AS VARCHAR) AS RETAILER_ID, CAST(\"Retailer Name\" AS VARCHAR) AS RETAILER_NAME, TRY_TO_NUMBER(SALES)*replace(c.MRP,\',\',\'\')::float as selling_price, upper(b.product_name) AS PRODUCT_NAME, UPPER(b.CATEGORY) AS PRODUCT_CATEGORY, upper(b.sub_Category) as product_sub_category FROM (select * from MAPLEMONK.FLIPKART_NEW_SALES_REPORT where TO_DATE(DATE) > (select max(TO_DATE(DATE)) from philips_db.MAPLEMONK.flipkart_historical_sales) ) a left join (select distinct \"FLIPKART FSN\", primarykey, category, sub_category, \"PRODUCT TITLE\" product_name from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MASTER) b on a.FSN = b.\"FLIPKART FSN\" left join ( select * from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MRP_COGS where lower(marketplace) = \'flipkart\' ) c on upper(trim(b.primarykey)) = upper(trim(c.sku_code)) AND DATE(TO_DATE(a.date)) BETWEEN cast(c.start_date as date) AND cast(c.end_date as date) union all select null AS EAN, product_id AS FSN, null AS HSN, null AS ISBN, TO_DATE(DATE) AS ORDER_DATE, null AS BRAND, quantity AS SALES_QTY, null AS MODEL_ID, null AS VERTICAL, null AS TENANT_ID, null AS PUBLISHER, null AS STYLE_CODE, null AS RETAILER_ID, null AS RETAILER_NAME, mcsi_value as selling_price, upper(b.product_name) as product_name, upper(bu) as product_category, upper(sag) as product_sub_Category from philips_db.maplemonk.flipkart_historical_sales a left join (select distinct \"FLIPKART FSN\", primarykey, category, sub_category, \"PRODUCT TITLE\" product_name from PHILIPS_DB.MAPLEMONK.PHILIPS_GS_SKU_MASTER) b on a.product_id = b.\"FLIPKART FSN\" ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PHILIPS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            