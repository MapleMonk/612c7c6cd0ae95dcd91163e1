{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS SOLARA_DB.MAPLEMONK.sku_master (skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE TABLE IF NOT EXISTS SOLARA_DB.MAPLEMONK.Custom_Amazon_Seller_Partner_Solara_Amazon_SC_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL ( Sku VARCHAR(16777216), Currency VARCHAR(16777216), \"item-tax\" VARCHAR(16777216), \"order-id\" VARCHAR(16777216), \"ship-city\" VARCHAR(16777216), \"buyer-name\" VARCHAR(16777216), \"item-price\" VARCHAR(16777216), \"ship-state\" VARCHAR(16777216), \"buyer-email\" VARCHAR(16777216), \"address-type\" VARCHAR(16777216), \"product-name\" VARCHAR(16777216), \"ship-country\" VARCHAR(16777216), \"gift-wrap-tax\" VARCHAR(16777216), \"license-state\" VARCHAR(16777216), \"licensee-name\" VARCHAR(16777216), \"order-channel\" VARCHAR(16777216), \"order-item-id\" VARCHAR(16777216), \"payments-date\" VARCHAR(16777216), \"purchase-date\" VARCHAR(16777216), \"gift-wrap-type\" VARCHAR(16777216), \"license-number\" VARCHAR(16777216), \"recipient-name\" VARCHAR(16777216), \"ship-address-1\" VARCHAR(16777216), \"ship-address-2\" VARCHAR(16777216), \"ship-address-3\" VARCHAR(16777216), \"shipping-price\" VARCHAR(16777216), \"number-of-items\" VARCHAR(16777216), \"gift-wrap-price\" VARCHAR(16777216), \"ship-postal-code\" VARCHAR(16777216), \"gift-message-text\" VARCHAR(16777216), \"is-business-order\" VARCHAR(16777216), \"is-global-express\" VARCHAR(16777216), \"item-promotion-id\" VARCHAR(16777216), \"price-designation\" VARCHAR(16777216), \"buyer-company-name\" VARCHAR(16777216), \"quantity-purchased\" VARCHAR(16777216), \"ship-service-level\" VARCHAR(16777216), \"delivery-instructions\" VARCHAR(16777216), \"purchase-order-number\" VARCHAR(16777216), \"shipping-promotion-id\" VARCHAR(16777216), \"order-channel-instance\" VARCHAR(16777216), \"item-promotion-discount\" VARCHAR(16777216), \"license-expiration-date\" VARCHAR(16777216), \"actual-ship-from-address-1\" VARCHAR(16777216), \"shipping-promotion-discount\" VARCHAR(16777216), \"actual-ship-from-address-city\" VARCHAR(16777216), \"actual-ship-from-address-name\" VARCHAR(16777216), \"actual-ship-from-address-state\" VARCHAR(16777216), \"default-ship-from-address-city\" VARCHAR(16777216), \"default-ship-from-address-name\" VARCHAR(16777216), \"default-ship-from-address-state\" VARCHAR(16777216), \"actual-ship-from-address-country\" VARCHAR(16777216), \"actual-ship-from-address-field-2\" VARCHAR(16777216), \"actual-ship-from-address-field-3\" VARCHAR(16777216), \"default-ship-from-address-country\" VARCHAR(16777216), \"default-ship-from-address-field-1\" VARCHAR(16777216), \"default-ship-from-address-field-2\" VARCHAR(16777216), \"default-ship-from-address-field-3\" VARCHAR(16777216), \"dctual-ship-from-address-postal-code\" VARCHAR(16777216), \"default-ship-from-address-postal-code\" VARCHAR(16777216) ) ; CREATE OR REPLACE TABLE SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_fact_items AS SELECT \'AMAZON_SELLER_CENTRAL_SOLARA\' AS shop_name, \"amazon-order-id\" AS order_id, NULL AS order_name, NULL AS customer_id, NULL AS phone, NULL AS tags, NULL AS line_item_id, sku, ASIN AS product_id, currency, case when \"order-status\" in (\'Shipped - Returned to Seller\', \'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\',\'Shipped - Damaged\') then 1 else 0 end AS is_refund, upper(\"ship-city\") AS city, upper(\"ship-state\") AS state, \"product-name\" AS product_name, NULL AS category, \"order-status\" AS order_status, \"Purchase-datetime-PDT\" AS order_timestamp, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) AS line_item_sales, ifnull(TRY_CAST(\"shipping-price\" AS FLOAT),0) AS shipping_price, ifnull(TRY_CAST(QUANTITY AS FLOAT),0) AS quantity, ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0) AS tax, div0(ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0),ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0)+ifnull(TRY_CAST(\"item-price\" AS FLOAT),0)) as tax_rate, ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS discount, div0(ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0),(1-tax_rate)) AS discount_before_tax, NULL AS gross_sales_after_tax, line_item_sales - discount_before_tax AS gross_sales_before_tax, NULL AS net_sales_before_tax, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) - ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS total_sales, \'Amazon\' AS source, NULL AS landing_utm_medium, NULL AS landing_utm_source, NULL AS landing_utm_campaign, NULL AS referring_utm_medium, NULL AS referring_utm_source, NULL AS landing_utm_channel, NULL AS referring_utm_channel, NULL AS final_utm_channel, NULL AS new_customer_flag, NULL AS acquisition_channel, NULL AS acquisition_product, ifnull(TRY_CAST(\"shipping-tax\" AS FLOAT),0) AS shipping_tax, ifnull(TRY_CAST(\"ship-promotion-discount\" AS FLOAT),0) AS ship_promotion_discount, ifnull(TRY_CAST(\"gift-wrap-price\" AS FLOAT),0) AS gift_wrap_price, ifnull(TRY_CAST(\"gift-wrap-tax\" AS FLOAT),0) AS gift_wrap_tax FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM SOLARA_DB.MAPLEMONK.Custom_Amazon_Seller_Partner_Solara_Amazon_SC_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL )X WHERE \"ship-country\" = \'IN\' AND \"item-price\" NOT IN(\'\',\'0.0\') ; create or replace table SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_orders_fact_items AS select * FROM SOLARA_DB.MAPLEMONK.Custom_Amazon_Seller_Partner_Solara_Amazon_SC_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL ; CREATE OR REPLACE TABLE SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_fact_items_TEMP_Category as select fi.* ,upper(coalesce(p.name,fi.product_name)) as product_name_final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS product_category ,Upper(p.sub_category) as product_sub_category ,AmazonOrdersBuyer.Buyer_email Email ,AmazonOrdersBuyer.Buyer_name Name ,AmazonOrdersBuyer.Recipient_Name from SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_fact_items fi left join (select * from (select skucode, name, category, sub_category, row_number() over (partition by skucode order by 1) rw from SOLARA_DB.MAPLEMONK.sku_master) where rw = 1 ) p on fi.sku = p.skucode left join (select \"order-id\" order_id ,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as Purchase_date ,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"payments-date\":: DATETIME) as Payments_date ,\"buyer-email\" Buyer_email ,\"buyer-name\" Buyer_name ,\"recipient-name\" Recipient_Name from SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_orders_fact_items) AmazonOrdersBuyer on fi.order_id=AmazonOrdersBuyer.order_id ; CREATE OR REPLACE TABLE SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_fact_items AS SELECT * FROM SOLARA_DB.MAPLEMONK.SOLARA_DB_amazon_fact_items_TEMP_Category;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SOLARA_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        