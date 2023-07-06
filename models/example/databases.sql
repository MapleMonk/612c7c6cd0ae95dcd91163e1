{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE beardo_db.MapleMonk.beardo_db_amazon_fact_items AS SELECT \'ASP_BEARDO_INDIA\' AS shop_name, \"amazon-order-id\" AS order_id, NULL AS order_name, NULL AS customer_id, NULL AS name, NULL AS email, NULL AS phone, NULL AS tags, NULL AS line_item_id, sku, ASIN AS product_id, currency, case when \"order-status\" in (\'Shipped - Returned to Seller\', \'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\',\'Shipped - Damaged\') then 1 else 0 end AS is_refund, upper(\"ship-city\") AS city, upper(\"ship-state\") AS state, \"product-name\" AS product_name, NULL AS category, \"order-status\" AS order_status, \"Purchase-datetime-PDT\" AS order_timestamp, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) AS line_item_sales, ifnull(TRY_CAST(\"shipping-price\" AS FLOAT),0) AS shipping_price, ifnull(TRY_CAST(QUANTITY AS FLOAT),0) AS quantity, ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0) AS tax, null as tax_rate, ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS discount, ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS discount_before_tax, line_item_sales gross_sales_after_tax, (line_item_sales - tax) as gross_sales_before_tax, (line_item_sales - tax -ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0)) net_sales_before_tax, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) - ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS total_sales, \'Amazon\' AS source, NULL AS landing_utm_medium, NULL AS landing_utm_source, NULL AS landing_utm_campaign, NULL AS referring_utm_medium, NULL AS referring_utm_source, NULL AS landing_utm_channel, NULL AS referring_utm_channel, NULL AS final_utm_channel, NULL AS new_customer_flag, NULL AS acquisition_channel, NULL AS acquisition_product, ifnull(TRY_CAST(\"shipping-tax\" AS FLOAT),0) AS shipping_tax, ifnull(TRY_CAST(\"ship-promotion-discount\" AS FLOAT),0) AS ship_promotion_discount, ifnull(TRY_CAST(\"gift-wrap-price\" AS FLOAT),0) AS gift_wrap_price, ifnull(TRY_CAST(\"gift-wrap-tax\"AS FLOAT),0) AS gift_wrap_tax FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM Beardo_db.MapleMonk.ASP_BEARDO_INDIA_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL )X ; CREATE OR REPLACE TABLE beardo_db.MapleMonk.beardo_db_amazon_fact_items_TEMP_Category as select fi.* ,product_name as product_name_final ,upper(fi.category) AS product_category ,null as product_sub_category from beardo_db.MapleMonk.beardo_db_amazon_fact_items fi ; CREATE OR REPLACE TABLE beardo_db.MapleMonk.beardo_db_amazon_fact_items AS SELECT * FROM beardo_db.MapleMonk.beardo_db_amazon_fact_items_TEMP_Category;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Beardo_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        