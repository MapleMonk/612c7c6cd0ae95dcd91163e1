{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table eggozdb.maplemonk.Offline_Business as WITH primary_table AS ( SELECT ROW_NUMBER() OVER (ORDER BY DATE) AS rn, DATE, RETAILER_NAME, AREA_CLASSIFICATION, PRODUCT_TYPE, RETAILER_CATEGORY, RETAILER_TYPE, DISTRIBUTOR, REVENUE, EGGS_SOLD, EGGS_REPLACED, EGGS_RETURN, SALE_TYPE FROM primary_and_secondary_sku WHERE RETAILER_CATEGORY NOT IN (\'Online MT\', \'Offline MT\', \'HORECA\') ), tertiary_table AS ( SELECT ROW_NUMBER() OVER (ORDER BY DELIVERY_DATE) AS rn, DELIVERY_DATE, DISTRIBUTOR_NAME, DEALER_NAME, CODE, CITY, AREA_CLASSIFICATION, SKU_SALE, EGGS_SOLD, PRODUCT_TYPE FROM tertiary_sales ), franchise_table AS ( SELECT ROW_NUMBER() OVER (ORDER BY CRM_DATE) AS rn, ID, CODE, CRM_DATE, FIRST_MONTH, PAYABLE_AMOUNT, CRM_EGGS_SOLD FROM crm_overview ) SELECT p.DATE, p.RETAILER_NAME, p.AREA_CLASSIFICATION, p.PRODUCT_TYPE, p.RETAILER_CATEGORY, p.RETAILER_TYPE, p.DISTRIBUTOR, p.REVENUE, p.EGGS_SOLD, p.EGGS_REPLACED, p.EGGS_RETURN, p.SALE_TYPE, t.DELIVERY_DATE AS TERTIARY_DELIVERY_DATE, t.DISTRIBUTOR_NAME AS TERTIARY_DISTRIBUTOR_NAME, t.DEALER_NAME AS TERTIARY_DEALER_NAME, t.CODE AS TERTIARY_CODE, t.CITY AS TERTIARY_CITY, t.AREA_CLASSIFICATION AS TERTIARY_AREA_CLASSIFICATION, t.SKU_SALE AS TERTIARY_SKU_SALE, t.EGGS_SOLD AS TERTIARY_EGGS_SOLD, t.PRODUCT_TYPE AS TERTIARY_PRODUCT_TYPE, f.ID AS FRANCHISE_ID, f.CODE AS FRANCHISE_CODE, f.CRM_DATE, f.FIRST_MONTH, f.PAYABLE_AMOUNT, f.CRM_EGGS_SOLD FROM primary_table p FULL OUTER JOIN tertiary_table t ON p.rn = t.rn FULL OUTER JOIN franchise_table f ON COALESCE(p.rn, t.rn) = f.rn;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            