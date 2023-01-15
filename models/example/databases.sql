{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS Perfora_DB.MapleMonk.sku_master (skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.Perfora_DB_amazon_fact_items AS SELECT \'Custom_Amazon_Seller_Partner_ASP___Perfora\' AS shop_name, \"amazon-order-id\" AS order_id, NULL AS order_name, NULL AS customer_id, NULL AS name, NULL AS email, NULL AS phone, NULL AS tags, NULL AS line_item_id, sku, ASIN AS product_id, currency, case when \"order-status\" in (\'Shipped - Returned to Seller\', \'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\',\'Shipped - Damaged\') then 1 else 0 end AS is_refund, upper(\"ship-city\") AS city, upper(\"ship-state\") AS state, \"product-name\" AS product_name, NULL AS category, \"order-status\" AS order_status, \"Purchase-datetime-PDT\" AS order_timestamp, TRY_CAST(\"item-price\" AS FLOAT) AS line_item_sales, TRY_CAST(\"shipping-price\" AS FLOAT) AS shipping_price, TRY_CAST(QUANTITY AS FLOAT) AS quantity, TRY_CAST(\"item-tax\" AS FLOAT) AS tax, null as tax_rate, TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS discount, TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS discount_before_tax, line_item_sales gross_sales_after_tax, (line_item_sales - tax) as gross_sales_before_tax, (line_item_sales - tax -TRY_CAST(\"item-promotion-discount\" AS FLOAT)) net_sales_before_tax, TRY_CAST(\"item-price\" AS FLOAT)-TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS total_sales, \'Amazon\' AS source, NULL AS landing_utm_medium, NULL AS landing_utm_source, NULL AS landing_utm_campaign, NULL AS referring_utm_medium, NULL AS referring_utm_source, NULL AS landing_utm_channel, NULL AS referring_utm_channel, NULL AS final_utm_channel, NULL AS new_customer_flag, NULL AS acquisition_channel, NULL AS acquisition_product, TRY_CAST(\"shipping-tax\" AS FLOAT) AS shipping_tax, TRY_CAST(\"ship-promotion-discount\" AS FLOAT) AS ship_promotion_discount, TRY_CAST(\"gift-wrap-price\" AS FLOAT) AS gift_wrap_price, TRY_CAST(\"gift-wrap-tax\" AS FLOAT) AS gift_wrap_tax FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM Perfora_DB.MapleMonk.Custom_Amazon_Seller_Partner_ASP___Perfora_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL )X WHERE \"ship-country\" = \'IN\' AND \"item-price\" NOT IN(\'\',\'0.0\') ; CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.Perfora_DB_amazon_fact_items_TEMP_Category as select fi.* ,p.name as product_name_final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS product_category ,Upper(p.sub_category) as product_sub_category from Perfora_DB.MapleMonk.Perfora_DB_amazon_fact_items fi left join (select & from (select skucode, name, category, sub_category, row_number() over (partition by SKUCODE order by SKUCODE) rw from Perfora_DB.MapleMonk.sku_master) where rw=1) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.Perfora_DB_amazon_fact_items AS SELECT * FROM Perfora_DB.MapleMonk.Perfora_DB_amazon_fact_items_TEMP_Category;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        