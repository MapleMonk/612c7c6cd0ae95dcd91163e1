{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SELECT_DB.MAPLEMONK.CLEANED_ABC_ABANDONED_CHECKOUTS AS WITH cte1 AS ( SELECT ID, NAME, EMAIL, PHONE, LANDING_SITE, CREATED_AT, UPDATED_AT, DISCOUNT_CODES, REFERRING_SITE, SUBTOTAL_PRICE, TOTAL_DISCOUNTS, TOTAL_LINE_ITEMS_PRICE, BUYER_ACCEPTS_MARKETING, COMPLETED_AT, _AIRBYTE_ABC_ABANDONED_CHECKOUTS_HASHID, ABANDONED_CHECKOUT_URL, A.value:\"compare_at_price\"::string AS COMPARE_AT_PRICE, A.value:\"quantity\"::integer AS QUANTITY, A.value:\"variant_id\"::integer AS VARIANT_ID, A.value:\"product_id\"::integer AS PRODUCT_ID, A.value:\"price\"::integer AS PRICE, A.value:\"presentment_title\"::string AS PRESENTMENT_TITLE, A.value:\"presentment_variant_title\"::string AS PRESENTMENT_VARIANT_TITLE, A.value:\"sku\"::string AS SKU, SUBSTRING(BILLING_ADDRESS, POSITION(\'city\' IN BILLING_ADDRESS) + 7, POSITION(\'\"\' IN SUBSTRING(BILLING_ADDRESS, POSITION(\'city\' IN BILLING_ADDRESS) + 7)) - 1) AS CITY, SUBSTRING(BILLING_ADDRESS, POSITION(\'zip\' IN BILLING_ADDRESS) + 6, POSITION(\'\"\' IN SUBSTRING(BILLING_ADDRESS, POSITION(\'zip\' IN BILLING_ADDRESS) + 6)) - 1) AS ZIP, ROW_NUMBER() OVER ( PARTITION BY NAME, EMAIL, PHONE, LANDING_SITE, CREATED_AT, UPDATED_AT, DISCOUNT_CODES, REFERRING_SITE, SUBTOTAL_PRICE, TOTAL_DISCOUNTS, TOTAL_LINE_ITEMS_PRICE, BUYER_ACCEPTS_MARKETING, COMPLETED_AT ORDER BY UPDATED_AT DESC ) AS row_num FROM select_db.maplemonk.abc_abandoned_checkouts, LATERAL FLATTEN (INPUT => LINE_ITEMS) A ) SELECT cte1.*, p.commonskuid, p.name AS PRODUCT_NAME, p.category, p.sub_category FROM cte1 LEFT JOIN ( SELECT * FROM ( SELECT marketplace_sku skucode, commonskuid, name, category, sub_category, ROW_NUMBER() OVER (PARTITION BY lower(marketplace_sku) ORDER BY 1) rw FROM SELECT_DB.MAPLEMONK.SELECT_DB_sku_master ) WHERE rw = 1 ) p ON lower(cte1.sku) = lower(p.skucode);",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        