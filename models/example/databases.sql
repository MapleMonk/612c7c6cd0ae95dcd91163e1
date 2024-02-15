{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE ABC_ACQUISITION_SOURCE AS select CREATED_AT, ID, NAME, EMAIL, PHONE, LANDING_SITE, UPDATED_AT, SUBSTRING(DISCOUNT_CODES, CHARINDEX(\'\"code\":\"\', DISCOUNT_CODES) + LEN(\'\"code\":\"\'), CHARINDEX(\'\"\', DISCOUNT_CODES, CHARINDEX(\'\"code\":\"\', DISCOUNT_CODES) + LEN(\'\"code\":\"\')) - CHARINDEX(\'\"code\":\"\', DISCOUNT_CODES) - LEN(\'\"code\":\"\')) AS DISCOUNT_CODES, SUBTOTAL_PRICE, TOTAL_DISCOUNTS, TOTAL_LINE_ITEMS_PRICE, SKU, PRESENTMENT_TITLE, PRESENTMENT_VARIANT_TITLE, CATEGORY, SUB_CATEGORY, PRICE, CASE WHEN TOTAL_LINE_ITEMS_PRICE = 0 THEN 0 ELSE (TOTAL_DISCOUNTS / TOTAL_LINE_ITEMS_PRICE) END AS DISCOUNT_PERCENTAGE, CASE WHEN TOTAL_LINE_ITEMS_PRICE = 0 THEN 0 ELSE (TOTAL_DISCOUNTS / TOTAL_LINE_ITEMS_PRICE) * PRICE END AS DISCOUNT_AMOUNT, CASE WHEN TOTAL_LINE_ITEMS_PRICE = 0 THEN PRICE ELSE PRICE - (TOTAL_DISCOUNTS / TOTAL_LINE_ITEMS_PRICE) * PRICE END AS PRICE_AFTER_DISCOUNT from SELECT_DB.MAPLEMONK.CLEANED_ABC_ABANDONED_CHECKOUTS",
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
                        