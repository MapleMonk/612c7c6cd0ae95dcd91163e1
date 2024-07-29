{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TEMPORARY TABLE temp_extracted_data AS SELECT saleorderdto, JSON_EXTRACT_ARRAY(saleorderdto, \'$.saleOrderItems\') AS saleOrderItems, JSON_EXTRACT_ARRAY(saleorderdto, \'$.shippingPackages\') AS shippingPackages, JSON_EXTRACT_ARRAY(saleorderdto, \'$.returns\') AS returns, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.pincode\') AS PINCODE, JSON_EXTRACT_SCALAR(saleorderdto, \'$.channel\') AS marketplace, JSON_EXTRACT_SCALAR(saleorderdto, \'$.source\') AS source, JSON_EXTRACT_SCALAR(saleorderdto, \'$.code\') AS order_id, JSON_EXTRACT_SCALAR(saleorderdto, \'$.displayOrderCode\') AS reference_code, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.phone\') AS phone, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.name\') AS name, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.email\') AS email, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(saleorderdto, \'$.updated\') AS int)), \"Asia/Kolkata\") AS SHIPPING_LAST_UPDATE_DATE, JSON_EXTRACT_SCALAR(saleorderdto, \'$.currencyCode\') AS currency, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.city\') AS city, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.state\') AS state, JSON_EXTRACT_SCALAR(saleorderdto, \'$.status\') AS ORDER_STATUS, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(saleorderdto, \'$.displayOrderDateTime\') AS int)), \"Asia/Kolkata\") AS order_Date, CASE WHEN REPLACE(JSON_EXTRACT_SCALAR(saleorderdto, \'$.cod\'), \'\"\', \'\') = \'true\' THEN \'COD\' ELSE \'Prepaid\' END AS payment_mode, CASE WHEN REPLACE(JSON_EXTRACT_SCALAR(saleorderdto, \'$.code\'), \'\"\', \'\') = \'true\' THEN \'COD\' ELSE \'Prepaid\' END AS payment_method FROM `MapleMonk.UNICOMMERCE_ZOUK_UC_GET_ORDERS_BY_IDS_final`; CREATE OR REPLACE TEMPORARY TABLE temp_order_related AS SELECT ed.PINCODE as pincode, ed.marketplace as marketplace, ed.source as source, ed.order_id as ordeR_id, ed.reference_code reference_code, ed.phone phone, ed.name name, ed.email email, ed.SHIPPING_LAST_UPDATE_DATE SHIPPING_LAST_UPDATE_DATE, JSON_EXTRACT_SCALAR(item, \'$.itemSku\') AS sku, JSON_EXTRACT_SCALAR(item, \'$.channelProductId\') AS product_id, JSON_EXTRACT_SCALAR(item, \'$.itemName\') AS product_name, ed.currency currency, ed.city city, ed.state state, ed.ORDER_STATUS order_status, ed.order_Date ordeR_date, CAST(JSON_EXTRACT_SCALAR(item, \'$.shippingCharges\') AS FLOAT64) AS shipping_price, CAST(JSON_EXTRACT_SCALAR(item, \'$.packetNumber\') AS FLOAT64) AS SUBORDER_QUANTITY, CAST(JSON_EXTRACT_SCALAR(item, \'$.discount\') AS FLOAT64) AS discount, CAST(JSON_EXTRACT_SCALAR(item, \'$.totalIntegratedGst\') AS FLOAT64) AS tax, CAST(JSON_EXTRACT_SCALAR(item, \'$.totalPrice\') AS FLOAT64) AS SELLING_PRICE, JSON_EXTRACT_SCALAR(item, \'$.shippingPackageCode\') AS shippingPackageCode, JSON_EXTRACT_SCALAR(item, \'$.shippingPackageStatus\') AS shippingPackageStatus, JSON_EXTRACT_SCALAR(item, \'$.code\') AS saleOrderItemCode, CAST(JSON_EXTRACT_SCALAR(item, \'$.id\') AS STRING) AS SALES_ORDER_ITEM_ID, JSON_EXTRACT_SCALAR(item, \'$.facilityName\') AS warehouse_name, JSON_EXTRACT_SCALAR(shippingPackages,\'$.trackingNumber\') AS AWB, ed.payment_mode, ed.payment_method FROM temp_extracted_data ed LEFT JOIN UNNEST(ed.saleOrderItems) AS item LEFT JOIN UNNEST(ed.shippingPackages) AS shippingPackages; CREATE OR REPLACE TEMPORARY TABLE temp_shipping_related AS WITH extracted_items AS ( SELECT REPLACE(JSON_EXTRACT_SCALAR(A, \'$.shippingProvider\'), \'\"\', \'\') AS courier, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.status\'), \'\"\', \'\') AS shipping_status, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(A, \'$.dispatched\') AS int)), \"Asia/Kolkata\") AS dispatched, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(A, \'$.delivered\') AS int)), \"Asia/Kolkata\") AS delivered, JSON_EXTRACT_SCALAR(A, \'$.code\') AS shippingPackageCode, c.order_id, JSON_EXTRACT_ARRAY(A, \'$.items\') AS items FROM temp_extracted_data c LEFT JOIN UNNEST(c.shippingPackages) A ) SELECT c.*, CAST(JSON_EXTRACT_SCALAR(B, \'$.quantity\') AS int64) AS shippedQuantity, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.itemSku\'), \'\"\', \'\') AS itemSku FROM extracted_items c LEFT JOIN UNNEST(items) B; CREATE OR REPLACE TEMPORARY TABLE temp_returns AS WITH extracted_return_data AS ( SELECT ed.order_id, ed.reference_code, JSON_EXTRACT_ARRAY(A, \'$.returnItems\') AS returnItems FROM temp_extracted_data ed LEFT JOIN UNNEST(ed.returns) AS A ) SELECT erd.order_id AS ordeR_id, erd.reference_code, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.saleOrderItemCode\'), \'\"\', \'\') AS saleOrderItemCode, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.itemSku\'), \'\"\', \'\') AS itemSku FROM extracted_return_data erd LEFT JOIN UNNEST(erd.returnItems) AS B WHERE REPLACE(JSON_EXTRACT_SCALAR(B, \'$.saleOrderItemCode\'), \'\"\', \'\') IS NOT NULL; create or replace table maplemonk.zouk_UNICOMMERCE_fact_items_intermediate_final as SELECT o.pincode, o.marketplace, o.source, o.order_id, o.reference_code, o.phone, o.name, o.email, o.SHIPPING_LAST_UPDATE_DATE, o.sku, o.product_id, o.product_name, o.currency, o.city, o.state, o.ORDER_STATUS, o.order_date, o.shipping_price, CASE WHEN LOWER(o.marketplace) LIKE \'%myntra%\' THEN s.shippedQuantity ELSE o.SUBORDER_QUANTITY END AS SUBORDER_QUANTITY, o.discount, o.tax, o.SELLING_PRICE, o.shippingPackageCode, o.shippingPackageStatus, o.saleOrderItemCode, o.SALES_ORDER_ITEM_ID, o.AWB, o.warehouse_name, o.payment_mode, o.payment_method, s.courier, s.shipping_status, s.shippedQuantity, s.dispatched AS Dispatch_date, s.delivered AS Delivered_Date, CASE WHEN r.itemSku IS NOT NULL THEN 1 ELSE 0 END AS return_flag, CASE WHEN (CASE WHEN r.itemSku IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN (CASE WHEN LOWER(o.marketplace) LIKE \'%myntra%\' THEN s.shippedQuantity ELSE o.SUBORDER_QUANTITY END) ELSE 0 END AS return_quantity, CASE WHEN order_status = \'CANCELLED\' THEN CASE WHEN LOWER(o.marketplace) LIKE \'%myntra%\' THEN s.shippedQuantity ELSE o.SUBORDER_QUANTITY END ELSE 0 END AS cancelled_quantity, CASE WHEN ROW_NUMBER() OVER (PARTITION BY phone ORDER BY order_date ASC) = 1 THEN \'New\' ELSE \'Repeat\' END AS new_customer_flag, FIRST_VALUE(product_name) OVER (PARTITION BY phone ORDER BY order_date ASC) AS acquisition_product, CASE WHEN UPPER(order_status) = \'COMPLETE\' THEN CAST(s.delivered AS DATE) - CAST(order_date AS DATE) ELSE CAST(CURRENT_DATE AS DATE) - CAST(order_date AS DATE) END AS days_in_shipment FROM temp_order_related o LEFT JOIN temp_shipping_related s ON o.shippingPackageCode = s.shippingPackageCode AND o.sku = s.itemSku AND o.order_id = s.order_id LEFT JOIN temp_returns r ON r.saleOrderItemCode = o.saleOrderItemCode AND r.order_id = o.order_id; create or replace table maplemonk.zouk_customerID_test_Final as with new_phone_numbers as ( select contact_num ,9700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct SUBSTR(REGEXP_REPLACE(REPLACE(phone, \' \', \'\'),\'[^a-zA-Z0-9]\',\'\'),-10) as contact_num from maplemonk.zouk_unicommerce_fact_items_intermediate_final ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,9800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,SUBSTR(REGEXP_REPLACE(REPLACE(e.contact_num, \' \', \'\'),\'[^a-zA-Z0-9]\',\'\'),-10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from maplemonk.zouk_unicommerce_fact_items_intermediate_final ) e left join new_phone_numbers p on p.contact_num = SUBSTR(REGEXP_REPLACE(REPLACE(e.contact_num, \' \', \'\'),\'[^a-zA-Z0-9]\',\'\'),-10) ) a ) b ) select contact_num,email,case when email is not null and email <> \'\' then min(maple_monk_id) over (partition by email ) else maple_monk_id end maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table maplemonk.zouk_unicommerce_fact_items as select coalesce(c.maple_monk_id,c.maple_monk_id) customer_id, o.*, min(order_date) over(partition by coalesce(c.maple_monk_id,c.maple_monk_id)) as acquisition_date from maplemonk.zouk_unicommerce_fact_items_intermediate_final o left join (select distinct contact_num phone,maple_monk_id from maplemonk.zouk_customerID_test_Final )c on replace(c.phone,\' \',\'\') = replace(o.phone,\' \',\'\'); CREATE TABLE IF NOT EXISTS maplemonk.zouk_SKU_MASTER ( skucode string, name string, category string, sub_category string); CREATE OR REPLACE TABLE maplemonk.zouk_unicommerce_fact_items_TEMP_Category as select fi.*, coalesce(p.SKUCODE,fi.SKU) AS SKU_CODE, coalesce(p.name,fi.product_name) as PRODUCT_NAME_Final, Upper(p.CATEGORY) AS Product_Category, Upper(p.SUB_CATEGORY) AS Product_Sub_Category from maplemonk.zouk_unicommerce_fact_items fi left join (select * from (select skucode, name, category, sub_category, row_number() over (partition by skucode order by 1) rw from maplemonk.zouk_sku_master) where rw = 1 ) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE maplemonk.zouk_unicommerce_fact_items AS SELECT * FROM maplemonk.zouk_unicommerce_fact_items_TEMP_Category; CREATE OR REPLACE TEMPORARY TABLE temp_extracted_data AS SELECT saleorderdto, JSON_EXTRACT_ARRAY(saleorderdto, \'$.returns\') AS returns, JSON_EXTRACT_ARRAY(saleorderdto, \'$.returns.returnItems\') AS returnItems, JSON_EXTRACT_ARRAY(saleorderdto, \'$.channel\') AS Marketplace, JSON_EXTRACT_ARRAY(saleorderdto, \'$.source\') AS Source, JSON_EXTRACT_SCALAR(saleorderdto, \'$.code\') AS order_id, JSON_EXTRACT_SCALAR(saleorderdto, \'$.displayOrderCode\') AS reference_code, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.phone\') AS phone, JSON_EXTRACT_SCALAR(saleorderdto, \'$.billingAddress.email\') AS email, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(saleorderdto, \'$.displayOrderDateTime\') AS int)), \"Asia/Kolkata\") AS order_date FROM `MapleMonk.UNICOMMERCE_ZOUK_UC_GET_ORDERS_BY_IDS_final`; CREATE OR REPLACE TEMPORARY TABLE temp_return_data AS SELECT ed.marketplace, ed.source, ed.order_id, ed.phone, ed.email, ed.order_date, ed.reference_code, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.code\'), \'\"\', \'\') AS return_display_code, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.statusCode\'), \'\"\', \'\') AS return_status, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(A, \'$.inventoryReceivedDate\') AS int)), \"Asia/Kolkata\") AS inventory_received_date, datetime(timestamp_millis(CAST(JSON_EXTRACT_SCALAR(A, \'$.returnCompletedDate\') AS int)), \"Asia/Kolkata\") AS return_complete_date, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.returnInvoiceDisplayCode\'), \'\"\', \'\') AS return_invoice_display_code, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.shippingProvider\'), \'\"\', \'\') AS return_courier, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.providerStatus\'), \'\"\', \'\') AS return_provider_shipping_status, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.trackingNumber\'), \'\"\', \'\') AS return_tracking_number, REPLACE(JSON_EXTRACT_SCALAR(A, \'$.type\'), \'\"\', \'\') AS return_type FROM temp_extracted_data ed LEFT JOIN UNNEST(ed.returns) AS A; CREATE OR REPLACE TEMPORARY TABLE temp_return_items AS SELECT ed.order_id, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.saleOrderItemCode\'), \'\"\', \'\') AS sale_order_item_code, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.itemSku\'), \'\"\', \'\') AS item_sku, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.itemname\'), \'\"\', \'\') AS item_name, REPLACE(JSON_EXTRACT_SCALAR(B, \'$.inventoryType\'), \'\"\', \'\') AS inventory_type FROM temp_extracted_data ed LEFT JOIN UNNEST(ed.returnItems) AS B; CREATE OR REPLACE TABLE maplemonk.zouk_UNICOMMERCE_RETURNS_INTERMEDIATE AS SELECT rd.marketplace, rd.source, rd.order_id, rd.phone, rd.email, rd.order_date, rd.reference_code, rd.return_display_code, rd.return_status, rd.inventory_received_date, rd.return_complete_date, rd.return_invoice_display_code, rd.return_courier, rd.return_provider_shipping_status, rd.return_tracking_number, rd.return_type, ri.sale_order_item_code, ri.item_sku, ri.item_name, ri.inventory_type FROM temp_return_data rd LEFT JOIN temp_return_items ri ON rd.order_id = ri.order_id;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            