create or replace table .._UNICOMMERCE_fact_items_intermediate_final as with Unicommerce_zouk_uc_order_related as ( select upper(replace(saleorderdto:billingAddress:pincode,\'\"\',\'\')) as pincode, replace(saleorderdto:channel,\'\"\',\'\') as marketplace, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:displayOrderCode,\'\"\',\'\') as reference_code, replace(saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(saleorderdto:billingAddress:name,\'\"\',\'\') as name, saleorderdto:billingAddress:email::varchar as email, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:updated,\'1970-01-01\')) SHIPPING_LAST_UPDATE_DATE, replace(A.Value:itemSku,\'\"\',\'\') as sku, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(A.Value:itemName,\'\"\',\'\') as product_name, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, upper(replace(saleorderdto:billingAddress:city,\'\"\',\'\')) as city, upper(replace(saleorderdto:billingAddress:state,\'\"\',\'\')) as state, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_date, A.Value:shippingCharges::float as shipping_price, A.Value:packetNumber::int as SUBORDER_QUANTITY, A.Value:discount::float as discount, A.Value:totalIntegratedGst::float as tax, A.Value:totalPrice::float as SELLING_PRICE, replace(A.Value:shippingPackageCode,\'\"\',\'\') as shippingPackageCode, replace(A.Value:shippingPackageStatus,\'\"\',\'\') as shippingPackageStatus, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, A.Value:id::varchar as SALES_ORDER_ITEM_ID, replace(B.Value:trackingNumber,\'\"\',\'\') as AWB, replace(A.Value:facilityName,\'\"\',\'\') warehouse_name, case when replace(saleorderdto:cod,\'\"\',\'\') = \'true\' then \'COD\' else \'Prepaid\' end as payment_mode, case when replace(saleorderdto:cod,\'\"\',\'\') = \'true\' then \'COD\' else \'Prepaid\' end as payment_method from ..Unicommerce_zouk_uc_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A, lateral flatten (INPUT => saleorderdto:shippingPackages)B ), Unicommerce_zouk_uc_shipping_related as ( select replace(A.Value:shippingProvider,\'\"\',\'\') as courier, replace(A.Value:status,\'\"\',\'\') shipping_status, A.Value:dispatched dispatched, A.Value:delivered delivered, A.Value:code as shippingPackageCode, B.Value:\"quantity\"::int as shippedQuantity, replace(c.saleorderdto:code,\'\"\',\'\') as order_id, replace(B.Value:\"itemSku\",\'\"\',\'\') as itemSku from ..Unicommerce_zouk_uc_GET_ORDERS_BY_IDS_TEST c, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A, LATERAL FLATTEN (INPUT => A.Value:items)B ), Unicommerce_zouk_uc_returns as ( select B.Value:saleOrderItemCode as saleOrderItemCode, B.Value:itemSku as itemSku, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from ..Unicommerce_zouk_uc_GET_ORDERS_BY_IDS_TEST C , LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) select o.marketplace, o.source, o.order_id, o.reference_code, o.phone, o.name, o.email, o.SHIPPING_LAST_UPDATE_DATE, o.sku, o.product_id, o.product_name, o.currency, o.city, o.state, o.ORDER_STATUS, o.order_date, o.shipping_price, case when lower(o.marketplace) like \'%myntra%\' then s.shippedquantity else o.SUBORDER_QUANTITY end SUBORDER_QUANTITY, o.discount, o.tax, o.SELLING_PRICE, o.shippingPackageCode, o.shippingPackageStatus, o.saleOrderItemCode, o.SALES_ORDER_ITEM_ID, o.AWB, o.warehouse_name, o.payment_mode, o.payment_method, s.courier, s.shipping_status, s.shippedQuantity, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.dispatched,\'1970-01-01\')) Dispatch_date, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.delivered,\'1970-01-01\'))) Delivered_Date, case when r.itemSku is not NUll then 1 else 0 end as return_flag, case when return_flag = 1 then (case when lower(o.marketplace) like \'%myntra%\' then s.shippedquantity else o.SUBORDER_QUANTITY end) else 0 end::int as return_quantity, case when order_status = \'CANCELLED\' then case when lower(o.marketplace) like \'%myntra%\' then s.shippedquantity else o.SUBORDER_QUANTITY end else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE( product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, case when UPPER(order_status)=\'COMPLETE\' then delivered_date-order_date::date else current_date - order_date::date end as days_in_shipment from Unicommerce_zouk_uc_order_related o left join Unicommerce_zouk_uc_shipping_related s on o.shippingPackageCode= s.shippingPackageCode and o.sku = s.itemSku and o.order_id=s.order_id left join Unicommerce_zouk_uc_returns r on r.saleOrderItemCode = o.saleOrderItemCode and r.order_id = o.order_id ; create or replace table .._customerID_test_Final as with new_phone_numbers as ( select contact_num ,9700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(replace(phone,\' \',\'\'), \'[^a-zA-Z0-9]+\'),10) as contact_num from .._unicommerce_fact_items_intermediate_final ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,9800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from .._unicommerce_fact_items_intermediate_final ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num,email,case when email is not null and email <> \'\' then min(maple_monk_id) over (partition by email ) else maple_monk_id end maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table .._unicommerce_fact_items as select coalesce(c.maple_monk_id,c.maple_monk_id) customer_id, o.*, min(order_date) over(partition by customer_id) as acquisition_date from .._unicommerce_fact_items_intermediate_final o left join (select distinct contact_num phone,maple_monk_id from .._customerID_test_Final )c on replace(c.phone,\' \',\'\') = replace(o.phone,\' \',\'\'); CREATE TABLE IF NOT EXISTS .._SKU_MASTER ( skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE OR REPLACE TABLE .._unicommerce_fact_items_TEMP_Category as select fi.*, coalesce(p.SKUCODE,fi.SKU) AS SKU_CODE, coalesce(p.name,fi.product_name) as PRODUCT_NAME_Final, Upper(p.CATEGORY) AS Product_Category, Upper(p.SUB_CATEGORY) AS Product_Sub_Category from .._unicommerce_fact_items fi left join (select * from (select skucode, name, category, sub_category, row_number() over (partition by skucode order by 1) rw from .._sku_master) where rw = 1 ) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE .._unicommerce_fact_items AS SELECT * FROM .._unicommerce_fact_items_TEMP_Category; create or replace table .._UNICOMMERCE_RETURNS_INTERMEDIATE as select replace(c.saleorderdto:channel,\'\"\',\'\') as marketplace, replace(c.saleorderdto:source,\'\"\',\'\') as source, replace(c.saleorderdto:code,\'\"\',\'\') as order_id, replace(c.saleorderdto:displayOrderCode,\'\"\',\'\') as reference_code, replace(c.saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(c.saleorderdto:billingAddress:name,\'\"\',\'\') as name, replace(c.saleorderdto:billingAddress:email,\'\"\',\'\') as email, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',c.saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_date, replace(A.value:code,\'\"\',\'\') as Return_DisplayCode, replace(A.value:statusCode,\'\"\',\'\') as Return_Status, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',A.value:inventoryReceivedDate,\'1970-01-01\')) Inventory_Received_Date, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',A.value:returnCompletedDate,\'1970-01-01\')) Return_Complete_Date, replace(A.value:returnInvoiceDisplayCode,\'\"\',\'\') as Return_Invoice_Display_Code, replace(A.value:shippingProvider,\'\"\',\'\') as Return_Courier, replace(A.value:providerStatus,\'\"\',\'\') as Return_Provider_Shipping_Status, replace(A.value:trackingNumber,\'\"\',\'\') as Return_Tracking_Number, replace(A.value:type,\'\"\',\'\') as Return_Type, replace(B.Value:saleOrderItemCode,\'\"\',\'\') as saleOrderItemCode, replace(B.Value:itemSku,\'\"\',\'\') as itemSku, replace(B.Value:itemName,\'\"\',\'\') Item_name, replace(B.Value:inventoryType,\'\"\',\'\') Inventory_Type from ..Unicommerce_zouk_uc_GET_ORDERS_BY_IDS_TEST C, LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ;