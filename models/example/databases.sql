{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ghc_db.maplemonk.amazon_fact_items_ghc as SELECT \'Amazon\' AS SHOP_NAME, \"amazon-order-id\" AS ORDER_ID, a.SKU, ASIN AS PRODUCT_ID, CURRENCY, \"ship-city\" AS CITY, \"ship-state\" AS STATE, \"ship-country\" AS COUNTRY, b.brand brand, b.category AS CATEGORY, coalesce(b.\"Product Name\",a.\"product-name\") product_name, \"item-status\" AS ITEM_STATUS, \"order-status\" AS ORDER_STATUS, \"Purchase-date-IST\"::date AS ORDER_date, TRY_CAST(\"item-price\" AS FLOAT) AS LINE_ITEM_SALES, TRY_CAST(\"shipping-price\" AS FLOAT) AS SHIPPING_PRICE, TRY_CAST(QUANTITY AS FLOAT) AS QUANTITY, TRY_CAST(\"item-tax\" AS FLOAT) AS TAX, TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS DISCOUNT, TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS DISCOUNT_BEFORE_TAX, NULL AS GROSS_SALES_AFTER_TAX, NULL AS GROSS_SALES_BEFORE_TAX, NULL AS NET_SALES_BEFORE_TAX, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0)-ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0)AS TOTAL_SALES, \"sales-channel\" AS sales_channel, TRY_CAST(\"shipping-tax\" AS FLOAT) AS SHIPPING_TAX, TRY_CAST(\"ship-promotion-discount\" AS FLOAT) AS SHIP_PROMOTION_DISCOUNT, TRY_CAST(\"gift-wrap-price\" AS FLOAT) AS GIFT_WRAP_PRICE, TRY_CAST(\"gift-wrap-tax\" AS FLOAT) AS GIFT_WRAP_TAX FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-date-IST\" from ghc_db.maplemonk.ASP_INDIA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL) a left join ghc_db.maplemonk.mapping_amazon_sales b on a.sku = b.sku ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GHC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        