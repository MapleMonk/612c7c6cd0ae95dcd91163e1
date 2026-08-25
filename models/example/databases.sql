{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.REDTAPE_DB_MARGIN_PRODUCT_MASTER AS WITH MASTER AS ( SELECT t.*, MAX(CASE WHEN f.value:field_name::STRING = \'ARTICLES\' THEN f.value:value::STRING END) AS ARTICLES, MAX(CASE WHEN f.value:field_name::STRING = \'CATEGORY-1\' THEN f.value:value::STRING END) AS CATEGORY_1, MAX(CASE WHEN f.value:field_name::STRING = \'CATEGORY-2\' THEN UPPER(REGEXP_REPLACE(f.value:value::STRING, \'^[-][-]|[-][-]$\', \'\')) END) AS CATEGORY_2, MAX(CASE WHEN f.value:field_name::STRING = \'GENDER\' THEN f.value:value::STRING END) AS GENDER, MAX(CASE WHEN f.value:field_name::STRING = \'Project_Name\' THEN f.value:value::STRING END) AS PROJECT_NAME, MAX(CASE WHEN f.value:field_name::STRING = \'Sub_Category\' THEN f.value:value::STRING END) AS SUB_CATEGORY, MAX(CASE WHEN f.value:field_name::STRING = \'Euro_Size\' THEN f.value:value::STRING END) AS EURO_SIZE, MAX(CASE WHEN f.value:field_name::STRING = \'UK_Size\' THEN f.value:value::STRING END) AS UK_SIZE, MAX(CASE WHEN f.value:field_name::STRING = \'US_Size\' THEN f.value:value::STRING END) AS US_SIZE FROM maplemonk.easyecom_redtape_product_master t, LATERAL FLATTEN(input => t.custom_fields, OUTER => TRUE) f GROUP BY ALL) SELECT M.MRP, M.SKU, M.ARTICLES, M.C_ID, M.COST, M.SIZE, M.BRAND, M.CP_ID, M.EANUPC, M.COLOUR, M.HEIGHT, M.LENGTH, M.WEIGHT, M.WIDTH, M.BRAND_ID, M.HSN_CODE, M.CREATED_AT, M.PRODUCT_ID, M.UPDATED_AT, M.CATEGORY_ID, M.DESCRIPTION, M.COMPANY_NAME, M.PRODUCT_NAME, M.PRODUCT_TYPE, M.CATEGORY_NAME, M.ACCOUNTING_SKU, M.CATEGORY_1 SAP_CATEGORY, UPPER(M.GENDER) GENDER, M.PROJECT_NAME, M.EURO_SIZE, M.UK_SIZE, M.US_SIZE, M.SUB_CATEGORY, CASE WHEN UPPER(TRIM(SUB_CATEGORY)) IN (\'CLOSE\', \'CLOSED\') THEN \'CLOSED\' WHEN UPPER(TRIM(SUB_CATEGORY)) = \'OPEN\' THEN \'OPEN\' WHEN UPPER(TRIM(SUB_CATEGORY)) IN (\'OPEN-OTHER\', \'OPEN - OTHER\') THEN \'OPEN-OTHERS\' WHEN UPPER(TRIM(SUB_CATEGORY)) IN(\'OPEN-SPORTS-SANDAL\' , \'OPEN - SPORTS SANDAL\')THEN \'OPEN-SPORT SANDAL\' ELSE UPPER(SUB_CATEGORY) END AS TYPE, M.CATEGORY_2, REGEXP_SUBSTR(CATEGORY_2, \'^[^-]+\') AS FLIPKART_CATEGORY, CASE WHEN REGEXP_COUNT(CATEGORY_2, \'-\') >= 2 THEN REGEXP_REPLACE(REGEXP_REPLACE(CATEGORY_2, \'^[^-]+-\', \'\'), \'-LEVEL [0-9]+$\', \'\') ELSE REGEXP_SUBSTR(CATEGORY_2, \'^[^-]+\') END AS MYNTRA_CATEGORY, REGEXP_SUBSTR(CATEGORY_2, \'LEVEL [0-9]+$\') AS LEVEL, FROM MASTER M QUALIFY ROW_NUMBER() OVER (PARTITION BY accounting_sku ORDER BY updated_at DESC) = 1;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            