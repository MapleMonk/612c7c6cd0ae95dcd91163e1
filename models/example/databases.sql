{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE TABLE IF NOT EXISTS HOX_DB.MAPLEMONK.sku_master (skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE TABLE IF NOT EXISTS HOX_DB.MAPLEMONK.Custom_Amazon_Seller_Partner_Amazon_BLANKO_HOX_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL ( Sku VARCHAR(16777216), Currency VARCHAR(16777216), \"item-tax\" VARCHAR(16777216), \"order-id\" VARCHAR(16777216), \"ship-city\" VARCHAR(16777216), \"buyer-name\" VARCHAR(16777216), \"item-price\" VARCHAR(16777216), \"ship-state\" VARCHAR(16777216), \"buyer-email\" VARCHAR(16777216), \"address-type\" VARCHAR(16777216), \"product-name\" VARCHAR(16777216), \"ship-country\" VARCHAR(16777216), \"gift-wrap-tax\" VARCHAR(16777216), \"license-state\" VARCHAR(16777216), \"licensee-name\" VARCHAR(16777216), \"order-channel\" VARCHAR(16777216), \"order-item-id\" VARCHAR(16777216), \"payments-date\" VARCHAR(16777216), \"purchase-date\" VARCHAR(16777216), \"gift-wrap-type\" VARCHAR(16777216), \"license-number\" VARCHAR(16777216), \"recipient-name\" VARCHAR(16777216), \"ship-address-1\" VARCHAR(16777216), \"ship-address-2\" VARCHAR(16777216), \"ship-address-3\" VARCHAR(16777216), \"shipping-price\" VARCHAR(16777216), \"number-of-items\" VARCHAR(16777216), \"gift-wrap-price\" VARCHAR(16777216), \"ship-postal-code\" VARCHAR(16777216), \"gift-message-text\" VARCHAR(16777216), \"is-business-order\" VARCHAR(16777216), \"is-global-express\" VARCHAR(16777216), \"item-promotion-id\" VARCHAR(16777216), \"price-designation\" VARCHAR(16777216), \"buyer-company-name\" VARCHAR(16777216), \"quantity-purchased\" VARCHAR(16777216), \"ship-service-level\" VARCHAR(16777216), \"delivery-instructions\" VARCHAR(16777216), \"purchase-order-number\" VARCHAR(16777216), \"shipping-promotion-id\" VARCHAR(16777216), \"order-channel-instance\" VARCHAR(16777216), \"item-promotion-discount\" VARCHAR(16777216), \"license-expiration-date\" VARCHAR(16777216), \"actual-ship-from-address-1\" VARCHAR(16777216), \"shipping-promotion-discount\" VARCHAR(16777216), \"actual-ship-from-address-city\" VARCHAR(16777216), \"actual-ship-from-address-name\" VARCHAR(16777216), \"actual-ship-from-address-state\" VARCHAR(16777216), \"default-ship-from-address-city\" VARCHAR(16777216), \"default-ship-from-address-name\" VARCHAR(16777216), \"default-ship-from-address-state\" VARCHAR(16777216), \"actual-ship-from-address-country\" VARCHAR(16777216), \"actual-ship-from-address-field-2\" VARCHAR(16777216), \"actual-ship-from-address-field-3\" VARCHAR(16777216), \"default-ship-from-address-country\" VARCHAR(16777216), \"default-ship-from-address-field-1\" VARCHAR(16777216), \"default-ship-from-address-field-2\" VARCHAR(16777216), \"default-ship-from-address-field-3\" VARCHAR(16777216), \"dctual-ship-from-address-postal-code\" VARCHAR(16777216), \"default-ship-from-address-postal-code\" VARCHAR(16777216) ) ; CREATE OR REPLACE TABLE HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items AS SELECT \'Amazon_BLANKO\' AS shop_name, \"amazon-order-id\" AS order_id, NULL AS order_name, NULL AS customer_id, NULL AS phone, NULL AS tags, NULL AS line_item_id, sku, ASIN AS product_id, currency, case when \"order-status\" in (\'Shipped - Returned to Seller\', \'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\',\'Shipped - Damaged\') then 1 else 0 end AS is_refund, upper(\"ship-city\") AS city, upper(\"ship-state\") AS state, \"product-name\" AS product_name, NULL AS category, \"order-status\" AS order_status, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) AS order_timestamp, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) AS line_item_sales, ifnull(TRY_CAST(\"shipping-price\" AS FLOAT),0) AS shipping_price, case when ifnull(TRY_CAST(QUANTITY AS FLOAT),0) = 0 then 1 else ifnull(TRY_CAST(QUANTITY AS FLOAT),0) end AS quantity, ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0) AS tax, div0(ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0),ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0)+ifnull(TRY_CAST(\"item-price\" AS FLOAT),0)) as tax_rate, ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS discount, div0(ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0),(1-tax_rate)) AS discount_before_tax, NULL AS gross_sales_after_tax, line_item_sales - discount_before_tax AS gross_sales_before_tax, NULL AS net_sales_before_tax, (ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) + ifnull(TRY_CAST(\"shipping-price\" AS FLOAT),0) - ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) - ifnull(TRY_CAST(\"ship-promotion-discount\" AS FLOAT),0)) AS total_sales, upper(\"fulfillment-channel\") AS source, NULL AS landing_utm_medium, NULL AS landing_utm_source, NULL AS landing_utm_campaign, NULL AS referring_utm_medium, NULL AS referring_utm_source, NULL AS landing_utm_channel, NULL AS referring_utm_channel, NULL AS final_utm_channel, NULL AS new_customer_flag, NULL AS acquisition_channel, NULL AS acquisition_product, ifnull(TRY_CAST(\"shipping-tax\" AS FLOAT),0) AS shipping_tax, ifnull(TRY_CAST(\"ship-promotion-discount\" AS FLOAT),0) AS ship_promotion_discount, ifnull(TRY_CAST(\"gift-wrap-price\" AS FLOAT),0) AS gift_wrap_price, ifnull(TRY_CAST(\"gift-wrap-tax\" AS FLOAT),0) AS gift_wrap_tax, upper(\"ship-service-level\") as shipment_level FROM HOX_DB.MAPLEMONK.Custom_Amazon_Seller_Partner_Amazon_BLANKO_HOX_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL as X where \"amazon-order-id\" not like \'S%\' ; create or replace table HOX_DB.MAPLEMONK.HOX_DB_amazon_orders_fact_items AS select * FROM HOX_DB.MAPLEMONK.Custom_Amazon_Seller_Partner_Amazon_BLANKO_HOX_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL ; CREATE OR REPLACE TABLE HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items_TEMP_Category as with sku_price_cancel as ( select distinct SKU, date_trunc(\'month\', order_timestamp :: date) as order_month, round(avg(line_item_sales/ quantity),2) as avg_sale_price from HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items where line_item_sales <> 0 group by 1,2 order by 1,2 ), sku_price_cancel_overall as ( select SKU, round(avg(line_item_sales/ quantity),2) as avg_overall_sale_price from HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items where line_item_sales <> 0 group by 1 order by 1 ), sale_data as ( select shop_name, order_id, order_name, customer_id, phone, tags, line_item_id, fi.sku, product_id, currency, is_refund, city, state, product_name, category, order_status, order_timestamp, case when line_item_sales = 0 then coalesce((quantity * avg_sale_price), (quantity * avg_overall_sale_price)) else line_item_sales end as line_item_sales, shipping_price, quantity, tax, tax_rate, discount, discount_before_tax, gross_sales_after_tax, gross_sales_before_tax, net_sales_before_tax, case when line_item_sales = 0 then (coalesce((quantity * avg_sale_price), (quantity * avg_overall_sale_price)) + shipping_price - ship_promotion_discount - discount) else total_sales end as total_sales, source, landing_utm_medium, landing_utm_source, landing_utm_campaign, referring_utm_medium, referring_utm_source, landing_utm_channel, referring_utm_channel, final_utm_channel, new_customer_flag, acquisition_channel, acquisition_product, shipping_tax, ship_promotion_discount, gift_wrap_price, gift_wrap_tax, shipment_level from HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items fi left join sku_price_cancel spc on fi.sku = spc.sku and date_trunc(\'month\', fi.order_timestamp :: date) = spc.order_month left join sku_price_cancel_overall spco on fi.sku = spco.sku ) select fi.*, coalesce(sku_master.master_sku,fi.sku) common_sku ,upper(coalesce(p.name, fi.product_name)) as product_name_final ,coalesce(Upper(p.CATEGORY),upper(fi.category)) AS product_category ,Upper(p.sub_category) as product_sub_category , AmazonOrdersBuyer.Buyer_email Email , AmazonOrdersBuyer.Buyer_name Name , AmazonOrdersBuyer.Recipient_Name from sale_data fi left join (select * from (select replace(marketplace_sku,\'\`\',\'\') marketplace_sku ,replace(master_sku,\'\`\',\'\') master_sku ,sub_category , category ,product_title ,row_number() over (partition by replace(marketplace_sku,\'\`\',\'\') order by 1) rw from HOX_DB.MAPLEMONK.sku_master ) where rw=1 ) sku_master on lower(fi.sku) = lower(sku_master.marketplace_sku) left join (select * from (select sku skucode, product_name name, category_name category, product_type sub_category, row_number() over (partition by sku order by 1) rw from HOX_DB.MAPLEMONK.easyecom_easyecom_blanko_hox_product_master) where rw = 1 ) p on lower(fi.sku) = lower(p.skucode) left join (select \"order-id\" order_id ,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as Purchase_date ,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"payments-date\":: DATETIME) as Payments_date ,\"buyer-email\" Buyer_email ,\"buyer-name\" Buyer_name ,\"recipient-name\" Recipient_Name from HOX_DB.MAPLEMONK.HOX_DB_amazon_orders_fact_items) AmazonOrdersBuyer on fi.order_id=AmazonOrdersBuyer.order_id ; CREATE OR REPLACE TABLE HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items AS SELECT * FROM HOX_DB.MAPLEMONK.HOX_DB_amazon_fact_items_TEMP_Category;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        