{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE table if not exists maplemonk.zouk_AMAZON_FACT_ITEMS ( Customer_id string,Shop_name string,Source string, order_id string, phone string, name string, email string, shipping_last_update_date string, sku string, product_id string, product_name string, currency string, city string, state string, order_status string, order_TIMESTAMP string, shipping_price FLOAT64, quantity FLOAT64, discount_before_tax FLOAT64, tax FLOAT64, total_Sales FLOAT64, is_refund INT64, product_name_final string, product_category string, product_sub_category string,pincode string) ; create table if not exists maplemonk.zouk_EasyEcom_FACT_ITEMS ( customer_id string, Shop_name string,marketplace string,Source string, order_id string, contact_num string, customer_name string, email string, shipping_last_update_date string, sku string, product_id string, productname string, currency string, city string, state string, order_status string, order_Date string, shipping_price FLOAT64, suborder_quantity FLOAT64, discount FLOAT64, tax FLOAT64, selling_price FLOAT64, is_refund INT64, suborder_id string, product_name_final string, product_category string, product_sub_category string, new_customer_flag string, shipping_status string, days_in_shipment string, awb string,Marketplace_LineItem_ID string, reference_code string,LAST_UPDATE_DATE date,PAYMENT_MODE string,COURIER string,MANIFEST_DATE date, DELIVERED_DATE date,mapped_product_name string,mapped_category string, mapped_sub_category string, warehouse_name string,pincode string) ; create table if not exists maplemonk.zouk_UNICOMMERCE_FACT_ITEMS (order_id string,order_Date date,reference_code string,name string,email string,city string,state string,phone string,saleorderitemcode string,sales_order_item_id string,shippingpackagecode string,SHIPPINGPACKAGESTATUS string,shipping_status string,order_status string,Courier string,Dispatch_Date date,Delivered_date date,Return_flag INT64,Return_quantity INT64,suborder_quantity INT64,cancelled_quantity INT64,selling_price FLOAT64,shipping_price FLOAT64,tax FLOAT64,discount FLOAT64,shipping_last_update_date date,days_in_shipment FLOAT64,awb string,marketplace string,payment_method string,PAYMENT_MODE string,PRODUCT_ID string,SKU string,SKU_CODE string,currency string,NEW_CUSTOMER_FLAG string,product_name string,mapped_product_name string,product_name_final string,mapped_category string,product_category string,mapped_sub_category string,product_sub_category string,warehouse_name string,pincode string); create table if not exists maplemonk.zouk_SHOPIFY_FACT_ITEMS (SHOPIFY_CUSTOMER_ID_FINAL INT64,SHOPIFY_ACQUISITION_DATE DATE,SHOPIFY_FIRST_COMPLETE_ORDER_DATE DATE,MAPLE_MONK_ID_PHONE INT64,SHOP_NAME string,MARKETPLACE string,ORDER_ID string,ORDER_NAME string,CUSTOMER_ID INT64,NAME string,EMAIL string,PHONE string,TAGS string,LINE_ITEM_ID ARRAY<STRUCT<field_name STRING>>,SKU string,PRODUCT_ID string,CURRENCY string,IS_REFUND INT64,SHIPPING_CITY string,SHIPPING_STATE string,CITY string,STATE string,PRODUCT_NAME string,CATEGORY string,ORDER_STATUS string,ORDER_TIMESTAMP TIMESTAMP,LINE_ITEM_SALES FLOAT64,QUANTITY FLOAT64,REFUND_QUANTITY INT64,REFUND_VALUE FLOAT64,TAX FLOAT64,TAX_RATE FLOAT64,DISCOUNT FLOAT64,DISCOUNT_BEFORE_TAX FLOAT64,GROSS_SALES_AFTER_TAX FLOAT64,GROSS_SALES_BEFORE_TAX FLOAT64,NET_SALES_BEFORE_TAX FLOAT64,SHIPPING_PRICE FLOAT64,TOTAL_SALES FLOAT64,SOURCE string,MOMENTS_COUNT INT64,DAYSTOCONVERT INT64,SHOPIFYQL_FIRSTVISIT_UTM_SOURCE string,SHOPIFYQL_MAPPED_SOURCE string,SHOPIFYQL_MAPPED_CHANNEL string,SHOPIFYQL_LAST_MOMENT_UTM_SOURCE string,SHOPIFYQL_LAST_VISIT_NON_UTM_SOURCE string,SHOPIFYQL_FIRSTVISIT_UTM_MEDIUM string,SHOPIFYQL_LAST_MOMENT_UTM_MEDIUM string,FINAL_UTM_CHANNEL string,FINAL_UTM_CAMPAIGN string,FINAL_UTM_SOURCE string,REFERRER_NAME string,CHECKOUT_MAPPED_SOURCE string,CHECKOUT_MAPPED_CHANNEL string,REFUND_DETAILS ARRAY<STRUCT<field_name STRING>>,AWB string,COURIER string,SHIPPING_STATUS string,SHIPPING_STATUS_UPDATE_DATE string,TRACKING_URL string,SHIPPING_CREATED_AT string,GATEWAY string,PAYMENT_MODE string,SKU_CODE string,COMMONSKUID string,PRODUCT_NAME_FINAL string,PRODUCT_CATEGORY string,PRODUCT_SUB_CATEGORY string,SHOPIFY_NEW_CUSTOMER_FLAG string,SHOPIFY_NEW_CUSTOMER_FLAG_MONTH string,SHOPIFY_ACQUISITION_PRODUCT string,SHOPIFY_ACQUISITION_CHANNEL string,SHOPIFY_ACQUISITION_SOURCE string,SHIPPING_TAX FLOAT64,SHIP_PROMOTION_DISCOUNT FLOAT64,GIFT_WRAP_PRICE FLOAT64,GIFT_WRAP_TAX FLOAT64,pincode string) ; CREATE or replace TABLE maplemonk.shipment_status_mapping (SHIPPING_STATUS string(16777216),Mapped_Status string(16777216)); Insert INTO maplemonk.shipment_status_mapping values (\'DELIVERED\',\'Delivered\'),(\'CANCELLED\',\'Cancelled\'),(\'DELIVERED TO ORIGIN\',\'Returned\'),(\'SHIPMENT CREATED\',\'Shipment Created\'),(\'RETURNED\',\'Returned\'),(\'IN TRANSIT\',\'In Transit\'),(\'DELIVERY DELAYED\',\'In Transit\'),(\'DELIVERED TO ORIGIN\',\'RTO\'),(\'CONFIRMED\',\'Open\'),(\'IN_TRANSIT\',\'In Transit\'),(\'RTO DELIVERED\',\'RTO\'),(\'CANCELED\',\'Cancelled\'),(\'RTO IN TRANSIT\',\'RTO\'),(\'REACHED AT DESTINATION HUB\',\'In Transit\'),(\'PICKED UP\',\'In Transit\'),(\'UNDELIVERED-2ND ATTEMPT\',\'In Transit\'),(\'RTO_OFD\',\'RTO\'),(\'OUT FOR DELIVERY\',\'Out for delivery\'),(\'RTO INITIATED\',\'RTO\'),(\'PICKUP EXCEPTION\',\'Pickup Error\'),(\'CANCELLATION REQUESTED\',\'Pickup Error\'),(\'READY TO SHIP\',\'Ready to Ship\'),(\'OUT_FOR_DELIVERY\',\'Out for delivery\'),(\'SHIPPED\',\'In Transit\'),(\'PICKUP RESCHEDULED\',\'Pickup Error\'),(\'UNDELIVERED-3RD ATTEMPT\',\'In Transit\'),(\'MISROUTED\',\'Misrouted\'),(\'PICKUP SCHEDULED\',\'Pickup Error\'),(\'UNDELIVERED-1ST ATTEMPT\',\'In Transit\'),(\'Not Traceble\',\'Pickup Error\'),(\'UNDELIVERED\',\'In Transit\'),(\'LOST\',\'Lost\'),(\'IN TRANSIT-EN-ROUTE\',\'In Transit\'),(\'Indian Speed Post\',\'Indian Speed Post\'),(\'Exception\',\'Exception\'),(\'Shipment Booked\',\'Pickup Error\'),(\'Assigned\',\'Assigned\'),(\'Pending\',\'Pending\'),(\'PrINT\',\'PrINT\'),(\'IN TRANSIT-AT DESTINATION HUB\',\'In Transit\'),(\'Upcoming\',\'Order Yet To sync\'),(\'RTO-In Transit\',\'RTO\'),(\'PICKUP ERROR\',\'Pickup Error\'),(\'RTO-Out for Delivery\',\'RTO\'),(\'RTO-Delivered\',\'RTO\'),(\'OUT FOR PICKUP\',\'Pickup Error\'),(\'RTO_NDR\',\'RTO\'),(\'RTO-Exception\',\'Exception\'),(\'Delayed\',\'In Transit\'),(\'IN TRANSIT-AT SOURCE HUB\',\'In Transit\'),(\'RTS\',\'Underprocess\'),(\'Manifested\',\'Pickup Error\'),(\'Dispatched\',\'In Transit\'),(\'RTO\',\'RTO\'),(\'Not Picked\',\'Pickup Error\'),(\'DAMAGED\',\'Damaged\'),(\'Closed\',\'Cancelled\'),(\'Open\',\'Open\'),(\'On Hold\',\'On Hold\'),(\'Shipped - Returned to Seller\',\'RTO\'),(\'Shipped - Delivered to Buyer\',\'Delivered\'),(\'Shipped - Picked Up\',\'In Transit\'),(\'Shipped - Rejected by Buyer\',\'RTO\'),(\'Shopify_Processed\',\'Shopify_Processed\'),(\'Ready to dispatch\',\'Ready to Dispatch\'),(\'RTO Undelivered\',\'RTO\'),(\'Shipment Lost\',\'Lost\'),(\'Shipment Error\',\'Shipment Error\'),(\'RTO In-Transit\',\'RTO\'),(\'Dispatch / RTO\',\'RTO\'),(\'Pending Pick-up\',\'Pending\'),(\'Dispatch/Intransit\',\'In Transit\'),(\'Dispatch /Lost and Damange\',\'Lost\'),(\'Dispatch /Undelivered\',\'Undelivered\'),(\'Non serviceable\',\'Non Serviceable\'),(\'Not serviceable\',\'Non Serviceable\'),(\'Dispatch/Delivered\',\'Delivered\'),(\'Refund\',\'Refund\'),(\'F&P\',\'F&P\'),(\'SHIPPED - RETURNING TO SELLER\',\'RTO\'),(\'SHIPPED - LOST IN TRANSIT\',\'Lost\'),(\'SHIPPED - DAMAGED\',\'Damaged\'),(\'SHIPPED - OUT FOR DELIVERY\',\'Out for delivery\'); create table if not exists maplemonk.facility_pincode (facility string,pincode string); create or replace table maplemonk.zouk_sales_consolidated_intermediate as select b.customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.marketplace) as marketplace ,Upper(coalesce(ccm.channel,b.FINAL_UTM_CHANNEL)) AS CHANNEL ,Upper(coalesce(b.source,b.FINAL_UTM_SOURCE)) AS SOURCE ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(cast(b.shipping_status_update_date as timestamp),cast(c.shipping_last_update_date as timestamp), cast(d.shipping_last_update_date as timestamp)) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,cast(TIMESTAMP(b.ORDER_TIMESTAMP) as date) AS Order_Date ,cast(TIMESTAMP(b.ORDER_TIMESTAMP) as datetime) AS Order_Time ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) FINAL_SHIPPING_STATUS ,cast(b.LINE_ITEM_ID as string) as SALEORDERITEMCODE ,d.sales_order_item_id as SALES_ORDER_ITEM_ID ,coalesce(b.awb,c.awb,d.awb) AWB ,UPPER(REPLACE(IF(ARRAY_LENGTH(GATEWAY) > 0, GATEWAY[OFFSET(0)],\'NA\'), \'\"\', \'\')) PAYMENT_GATEWAY ,upper(coalesce(c.payment_mode,d.payment_mode)) Payment_Mode ,Upper(coalesce(c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(cast(timestamp(b.Shipping_created_at) as date),cast(timestamp(c.manifest_date) as date),cast(timestamp(d.dispatch_date) as date)) AS DISPATCH_DATE ,coalesce(cast(timestamp(c.delivered_date) as date),cast(timestamp(d.delivered_date) as date),case when b.shipping_status like \'delivered\' then cast(timestamp(b.shipping_status_update_date) as date) end) AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_quantity,0) end returned_quantity ,case when coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then cast(quantity as INT64) end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,cast(case when lower(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) in (\'delivered\',\'delivered to origin\') then date_diff(date(b.ORDER_TIMESTAMP),date(coalesce(cast(b.shipping_status_update_date as timestamp),cast(c.shipping_last_update_date as timestamp), cast(d.shipping_last_update_date as timestamp))),day) when lower(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) in (\'in transit\', \'shipment created\') then date_diff(date(b.ORDER_TIMESTAMP), CURRENT_DATE(),day) end as INT64) as Days_in_Shipment ,b.shopify_acquisition_date AS MARKETPLACE_ACQUISITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,b.COLLECTION ,b.PRINT ,b.PRODUCT_TYPE ,b.category_code ,b.BAU_OFFLINE ,b.BAU_ONLINE ,b.FINAL_TAX_RATE ,b.SKU_CODE commonsku ,upper(coalesce(c.warehouse_name,d.warehouse_name)) WAREHOUSE ,coalesce(b.pincode,d.pincode,c.pincode) as pincode ,coalesce(c.pickup_pincode,fa.pincode) as source_pincode ,b.FINAL_MARKETPLACE ,b.MARKETPLACE_SEGMENTs as MARKETPLACE_SEGMENT ,b.TYPE_OF_SALE ,b.discount_codes ,ccm.source as copoun_source from maplemonk.zouk_SHOPIFY_FACT_ITEMS b left join `MapleMonk.Coupon_Code_Mapping` ccm on lower(b.discount_codes) like lower(ccm.Coupon_Code_Prefix) || \'%\' left join (select * from ( select * ,row_number()over(partition by reference_code, Marketplace_LineItem_ID order by last_update_date desc) rw from maplemonk.zouk_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%shopify%\') ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID left join (select * from (select * ,row_number() over (partition by order_id, SPLIT(saleorderitemcode, \'-\')[OFFSET(0)] order by shipping_last_update_date desc) rw from maplemonk.zouk_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=SPLIT(saleorderitemcode, \'-\')[OFFSET(0)] left join (select * from (select * , row_number() over (partition by lower(facility) order by 1) rw from maplemonk.facility_pincode ) where rw = 1 ) fa on lower(d.warehouse_name) = lower(fa.facility) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status,c.shipping_status,d.shipping_status,b.ORDER_STATUS)) = lower(ShipMap.shipping_status) UNION ALL SELECT cast(NULL AS string) customer_id, UPPER(afi.SHOP_NAME) AS Shop_name, \'AMAZON\' AS marketplace, \'AMAZON\' AS CHANNEL, \'AMAZON\' AS SOURCE, afi.ORDER_ID, afi.ORDER_ID AS reference_code, NULL AS PHONE, afi.NAME, COALESCE(EEFI.EMAIL, UFI.EMAIL, AFI.EMAIL) AS EMAIL, COALESCE( CAST(EEFI.shipping_last_update_date AS timestamp), CAST(UFI.shipping_last_update_date AS timestamp) ) AS SHIPPING_LAST_UPDATE_DATE, afi.SKU, cast(afi.PRODUCT_ID as string) PRODUCT_ID, afi.PRODUCT_NAME, afi.CURRENCY, UPPER(afi.CITY) AS CITY, UPPER(afi.STATE) AS State, UPPER(afi.ORDER_STATUS) AS Order_Status, CAST(afi.ORDER_TIMESTAMP AS DATE) AS Order_Date, CAST(afi.ORDER_TIMESTAMP AS datetime) Order_Time, afi.QUANTITY, IFNULL(TOTAL_SALES, 0) - IFNULL(afi.tax, 0) + IFNULL(DISCOUNT_BEFORE_TAX, 0) AS GROSS_SALES_BEFORE_TAX, DISCOUNT_BEFORE_TAX AS DISCOUNT, afi.TAX, afi.SHIPPING_PRICE, TOTAL_SALES AS SELLING_PRICE, UPPER(COALESCE(EEFI.order_status, UFI.order_status)) AS OMS_order_status, UPPER(COALESCE(EEFI.shipping_status, UFI.shipping_status)) AS SHIPPING_STATUS, UPPER(COALESCE(shipmap.final_shipping_status, EEFI.shipping_status, UFI.shipping_status)) AS FINAL_SHIPPING_STATUS, CONCAT(afi.ORDER_ID, \'-\', afi.PRODUCT_ID) AS SALEORDERITEMCODE, CONCAT(afi.ORDER_ID, \'-\', afi.PRODUCT_ID) AS SALES_ORDER_ITEM_ID, COALESCE(EEFI.awb, UFI.awb) AS AWB, NULL AS Payment_Gateway, UPPER(COALESCE(EEFI.payment_mode, UFI.payment_mode)) AS Payment_Mode, UPPER(COALESCE(EEFI.Courier, UFI.courier)) AS COURIER, COALESCE(EEFI.manifest_date, UFI.dispatch_date) AS DISPATCH_DATE, COALESCE(EEFI.delivered_date , UFI.delivered_date ) AS DELIVERED_DATE, CASE WHEN LOWER(COALESCE(shipmap.final_shipping_status, UFI.shipping_status, EEFI.shipping_status)) = \'delivered\' THEN 1 ELSE 0 END AS DELIVERED_STATUS, afi.IS_REFUND AS RETURN_FLAG, CASE WHEN afi.is_refund = 1 THEN CAST(afi.quantity AS INT64) END AS returned_quantity, CASE WHEN afi.is_refund = 1 THEN afi.total_sales END AS returned_sales, CASE WHEN afi.is_refund = 0 AND LOWER(afi.order_status) = \'cancelled\' THEN CAST(afi.quantity AS INT64) END AS cancelled_quantity, NULL AS NEW_CUSTOMER_FLAG, NULL AS ACQUISITION_PRODUCT, CASE WHEN LOWER(COALESCE(shipmap.final_shipping_status, EEFI.shipping_status, UFI.shipping_status)) IN (\'delivered\', \'delivered to origin\') THEN DATE_DIFF(CAST(CURRENT_DATE() AS DATE), CAST(afi.ORDER_TIMESTAMP AS DATE), DAY) WHEN LOWER(COALESCE(shipmap.final_shipping_status, EEFI.shipping_status, UFI.shipping_status)) IN (\'in transit\', \'shipment created\') THEN DATE_DIFF(CAST(CURRENT_DATE() AS DATE), CAST(afi.ORDER_TIMESTAMP AS DATE), DAY) END AS Days_in_Shipment, NULL AS ACQUISITION_DATE, COALESCE(afi.SKU, UFI.PRODUCT_ID, EEFI.SKU) AS SKU_CODE, UPPER(AFI.PRODUCT_NAME_FINAL) AS PRODUCT_NAME_FINAL, UPPER(AFI.PRODUCT_CATEGORY) AS PRODUCT_CATEGORY, UPPER(AFI.PRODUCT_SUB_CATEGORY) AS PRODUCT_SUB_CATEGORY, AFI.COLLECTION ,AFI.PRINT ,AFI.PRODUCT_TYPE ,AFI.category_code ,AFI.BAU_OFFLINE ,AFI.BAU_ONLINE ,AFI.FINAL_TAX_RATE ,AFI.commonsku ,UPPER(coalesce(EEFI.warehouse_name, UFI.warehouse_name)) AS WAREHOUSE, COALESCE(AFI.pincode, UFI.pincode, EEFI.pincode) AS pincode, coalesce(EEFI.pickup_pincode, fa.pincode) source_pincode ,\'AMAZON\' FINAL_MARKETPLACE ,\'MARKETPLACE\' MARKETPLACE_SEGMENT ,\'SALES\' TYPE_OF_SALE ,null as discount_codes ,null as copoun_source FROM maplemonk.zouk_AMAZON_FACT_ITEMS AFI LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY reference_code, SKU ORDER BY last_update_date DESC) AS rw FROM maplemonk.zouk_EasyEcom_FACT_ITEMS ) z WHERE z.rw = 1 AND LOWER(marketplace) LIKE \'%amazon%\' ) EEFI ON AFI.Order_id = EEFI.reference_code AND AFI.SKU = EEFI.sku LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY shipping_last_update_date DESC) AS rw FROM maplemonk.zouk_UNICOMMERCE_FACT_ITEMS WHERE LOWER(marketplace) LIKE \'%amazon%\' ) ufi WHERE ufi.rw = 1 ) UFI ON cast(AFI.order_id as string) = cast(UFI.order_id as string) AND cast(AFI.product_id as string) = cast(UFI.PRODUCT_ID as string) left join (select * from (select * , row_number() over (partition by lower(facility) order by 1) rw from maplemonk.facility_pincode ) where rw = 1 ) fa on lower(ufi.warehouse_name) = lower(fa.facility) LEFT JOIN ( SELECT * FROM ( SELECT UPPER(Shipping_status) AS shipping_status, UPPER(mapped_status) AS final_shipping_status, ROW_NUMBER() OVER (PARTITION BY LOWER(shipping_Status) ORDER BY 1) AS rw FROM maplemonk.shipment_status_mapping ) shipmap WHERE shipmap.rw = 1 ) ShipMap ON LOWER(COALESCE(EEFI.shipping_status, UFI.shipping_status, afi.ORDER_STATUS)) = LOWER(ShipMap.shipping_status) UNION ALL SELECT NULL AS customer_id ,UPPER(SHOP_NAME) AS SHOP_NAME ,UPPER(marketplace) AS marketplace ,UPPER(marketplace) AS CHANNEL ,UPPER(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,contact_num AS PHONE ,customer_name AS NAME ,email AS EMAIL ,cast(shipping_last_update_date as timestamp) AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,UPPER(PRODUCTNAME) AS PRODUCT_NAME ,CURRENCY ,UPPER(CITY) AS City ,UPPER(STATE) AS State ,UPPER(ORDER_STATUS) AS Order_Status ,cast(cast(ORDER_DATE as datetime) as date) AS Order_Date ,CAST(ORDER_DATE AS datetime) AS ORDER_TIMESTAMP ,SUBORDER_QUANTITY AS QUANTITY ,IFNULL(SELLING_PRICE, 0) - IFNULL(tax, 0) + IFNULL(DISCOUNT, 0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,UPPER(ORDER_STATUS) AS OMS_Order_Status ,UPPER(b.Shipping_status) AS SHIPPING_STATUS ,UPPER(COALESCE(shipmap.final_shipping_status, b.shipping_status)) AS FINAL_SHIPPING_STATUS ,Marketplace_LineItem_ID AS SALEORDERITEMCODE ,suborder_id AS SALES_ORDER_ITEM_ID ,AWB ,NULL AS Payment_Gateway ,payment_mode AS Payment_Mode ,UPPER(COURIER) AS COURIER ,MANIFEST_DATE AS DISPATCH_DATE ,DELIVERED_DATE ,CASE WHEN LOWER(COALESCE(ShipMap.shipping_status, b.shipping_status)) = \'delivered\' THEN 1 ELSE 0 END AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,CASE WHEN is_refund = 1 THEN CAST(suborder_quantity AS INT64) END AS returned_quantity ,CASE WHEN is_refund = 1 AND LOWER(order_status) NOT IN (\'cancelled\') THEN IFNULL(is_refund, 0) END AS returned_sales ,CASE WHEN is_refund = 0 AND LOWER(order_status) IN (\'cancelled\') THEN CAST(suborder_quantity AS INT64) END AS cancelled_quantity ,CAST(new_customer_flag AS STRING) AS NEW_CUSTOMER_FLAG ,NULL AS ACQUISITION_PRODUCT ,cast(Days_in_shipment as INT64) AS DAYS_IN_SHIPMENT ,NULL AS ACQUISITION_DATE ,SKU AS SKU_CODE ,UPPER(mapped_product_name) AS PRODUCT_NAME_FINAL ,UPPER(mapped_category) AS PRODUCT_CATEGORY ,UPPER(mapped_sub_category) AS PRODUCT_SUB_CATEGORY ,b.COLLECTION ,b.PRINT ,b.PRODUCT_TYPE ,b.category_code ,b.BAU_OFFLINE ,b.BAU_ONLINE ,b.FINAL_TAX_RATE ,b.commonsku ,UPPER(WAREHOUSE_NAME) AS WAREHOUSE ,pincode ,pickup_pincode ,b.FINAL_MARKETPLACE ,b.MARKETPLACE_SEGMENT ,b.TYPE_OF_SALE ,null as discount_codes ,null as copoun_source FROM maplemonk.zouk_EasyEcom_FACT_ITEMS b LEFT JOIN ( SELECT * FROM ( SELECT UPPER(Shipping_status) AS shipping_status, UPPER(mapped_status) AS final_shipping_status, ROW_NUMBER() OVER (PARTITION BY LOWER(shipping_Status) ORDER BY 1) AS rw FROM maplemonk.shipment_status_mapping ) shipmap WHERE shipmap.rw = 1 ) ShipMap ON LOWER(COALESCE(b.shipping_status, b.order_status)) = LOWER(ShipMap.shipping_status) WHERE not(LOWER(marketplace) LIKE \'%shopify%\' or LOWER(marketplace) LIKE ANY (\'%amazon%\', \'%gofynd\') or LOWER(ifnull(TYPE_OF_SALE,\'1\')) LIKE ANY (\'na\',\'%stock%\')) UNION ALL SELECT NULL AS customer_id ,UPPER(marketplace) AS shop_name ,UPPER(marketplace) AS marektplace ,UPPER(marketplace) AS CHANNEL ,UPPER(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,phone AS PHONE ,name AS NAME ,email AS EMAIL ,cast(shipping_last_update_date as timestamp) AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.PRODUCT_ID ,PRODUCT_NAME AS PRODUCT_NAME ,CURRENCY ,UPPER(CITY) AS city ,UPPER(STATE) AS State ,UPPER(ORDER_STATUS) AS order_status ,CAST(ORDER_DATE AS DATE) AS Order_Date ,CAST(ORDER_DATE AS datetime) ORDER_TIME ,SUBORDER_QUANTITY AS QUANTITY ,IFNULL(SELLING_PRICE, 0) - IFNULL(tax, 0) AS gross_sales_before_tax ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,UPPER(ORDER_STATUS) AS OMS_ORDER_STATUS ,UPPER(b.shipping_status) AS SHIPPING_STATUS ,UPPER(COALESCE(shipmap.final_shipping_status, b.shipping_status)) AS FINAL_SHIPPING_STATUS ,saleOrderItemCode AS SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID AS SALES_ORDER_ITEM_ID ,AWB ,NULL AS payment_gateway ,payment_mode ,COURIER ,DISPATCH_DATE AS DISPATCH_DATE ,delivered_date AS delivered_date ,CASE WHEN UPPER(FINAL_SHIPPING_STATUS) IN (\'DELIVERED\') THEN 1 END AS DELIVERED_STATUS ,return_flag AS RETURN_FLAG ,CASE WHEN return_flag = 1 THEN CAST(suborder_quantity AS INT64) END AS returned_quantity ,CASE WHEN return_flag = 1 THEN CAST(selling_price AS FLOAT64) END AS returned_sales ,CASE WHEN return_flag = 0 AND LOWER(order_status) IN (\'cancelled\') THEN CAST(suborder_quantity AS INT64) END AS cancelled_quantity ,CAST(new_customer_flag AS STRING) AS NEW_CUSTOMER_FLAG ,NULL AS ACQUISITION_PRODUCT ,CASE WHEN order_status = \'COMPLETE\' THEN date_diff(CAST(delivered_date AS DATE), CAST(order_date AS DATE),day) ELSE date_diff(CURRENT_DATE(), CAST(order_date AS DATE),day) END AS days_in_shipment ,NULL AS ACQUISITION_DATE ,sku_code ,UPPER(b.product_name_final) AS PRODUCT_NAME_FINAL ,UPPER(b.Product_Category) AS PRODUCT_CATEGORY ,UPPER(b.product_sub_category) AS PRODUCT_SUB_CATEGORY ,b.COLLECTION ,b.PRINT ,b.PRODUCT_TYPE ,b.category_code ,b.BAU_OFFLINE ,b.BAU_ONLINE ,b.FINAL_TAX_RATE ,b.commonsku ,UPPER(warehouse_name) AS warehouse ,b.pincode ,fa.pincode source_pincode ,b.FINAL_MARKETPLACE ,b.MARKETPLACE_SEGMENT ,b.TYPE_OF_SALE ,null discount_codes ,null as copoun_source FROM maplemonk.zouk_unicommerce_fact_items b left join (select * from (select * , row_number() over (partition by lower(facility) order by 1) rw from maplemonk.facility_pincode ) where rw = 1 ) fa on lower(b.warehouse_name) = lower(fa.facility) LEFT JOIN ( SELECT * FROM ( SELECT UPPER(Shipping_status) AS shipping_status, UPPER(mapped_status) AS final_shipping_status, ROW_NUMBER() OVER (PARTITION BY LOWER(shipping_Status) ORDER BY 1) AS rw FROM maplemonk.shipment_status_mapping ) ShipMap WHERE ShipMap.rw = 1 ) ShipMap ON LOWER(COALESCE(b.shipping_status, b.order_status)) = LOWER(ShipMap.shipping_status) WHERE NOT (LOWER(b.marketplace) LIKE ANY (\'%shopify%\') or LOWER(marketplace) LIKE ANY (\'%amazon%\', \'%gofynd\') or LOWER(ifnull(TYPE_OF_SALE,\'1\')) LIKE ANY (\'na\',\'%stock%\')) union all SELECT NULL AS customer_id ,\'MYNTRA_SJIT\' AS shop_name ,\'MYNTRA_SJIT\' AS marektplace ,\'MYNTRA_SJIT\' AS CHANNEL ,\'MYNTRA_SJIT\' AS SOURCE ,cast(seller_order_id as string) ORDER_ID ,cast(order_id_fk as string) reference_code ,null AS PHONE ,null AS NAME ,null AS EMAIL ,cast(created_on as timestamp) AS SHIPPING_LAST_UPDATE_DATE ,sku_id as SKU ,style_id PRODUCT_ID ,style_name AS PRODUCT_NAME ,\'IN\' CURRENCY ,UPPER(CITY) AS city ,UPPER(STATE) AS State ,UPPER(ORDER_STATUS) AS order_status ,CAST(created_on AS DATE) AS Order_Date ,CAST(created_on AS datetime) ORDER_TIME ,1 AS QUANTITY ,IFNULL(cast(final_amount as int64),0) - IFNULL(cast(tax_recovery as int64), 0) AS gross_sales_before_tax ,cast(DISCOUNT as float64) AS DISCOUNT ,cast(tax_recovery as float64) TAX ,cast(shipping_charge as float64) as SHIPPING_PRICE ,cast(final_amount as float64) AS SELLING_PRICE ,UPPER(ORDER_STATUS) AS OMS_ORDER_STATUS ,UPPER(ORDER_STATUS) AS SHIPPING_STATUS ,ORDER_STATUS AS FINAL_SHIPPING_STATUS ,order_line_id AS SALEORDERITEMCODE ,order_line_id AS SALES_ORDER_ITEM_ID ,order_tracking_number as AWB ,NULL AS payment_gateway ,null as payment_mode , courier_code COURIER ,null AS DISPATCH_DATE ,null AS delivered_date ,CASE WHEN UPPER(ORDER_STATUS) IN (\'DELIVERED\') THEN 1 END AS DELIVERED_STATUS , case when return_creation_date is null then 0 else 1 end AS RETURN_FLAG ,CASE WHEN (case when return_creation_date is null then 0 else 1 end ) = 1 THEN CAST(1 AS INT64) END AS returned_quantity ,CASE WHEN (case when return_creation_date is null then 0 else 1 end ) = 1 THEN CAST(final_amount AS FLOAT64) END AS returned_sales ,CASE WHEN return_creation_date is null AND cancelled_on is not null THEN CAST(1 AS INT64) END AS cancelled_quantity ,null AS NEW_CUSTOMER_FLAG ,NULL AS ACQUISITION_PRODUCT ,CASE WHEN order_status = \'COMPLETE\' THEN date_diff(CAST(delivered_on AS DATE), CAST(created_on AS DATE),day) ELSE date_diff(CURRENT_DATE(), CAST(created_on AS DATE),day) END AS days_in_shipment ,NULL AS ACQUISITION_DATE ,myntra_sku_code as sku_code ,name AS PRODUCT_NAME_FINAL ,category AS PRODUCT_CATEGORY ,sub_category AS PRODUCT_SUB_CATEGORY ,collection as COLLECTION ,print as PRINT ,PRODUCT_TYPE as PRODUCT_TYPE ,category_code category_code ,BAU_OFFLINE BAU_OFFLINE ,BAU_ONLINE BAU_ONLINE ,TAX_RATE as FINAL_TAX_RATE ,commonsku ,UPPER(warehouse_id) AS warehouse ,zipcode as pincode ,null source_pincode ,\'MYNTRA\' FINAL_MARKETPLACE ,\'MARKETPLACE\' MARKETPLACE_SEGMENT ,\'SALES\' TYPE_OF_SALE ,null as discount_codes ,null as copoun_source from maplemonk.zouk_myntra_sjit_orders fi left join (select * from (select marketplace_sku skucode, name, category, sub_category, category_code, collection, print, PRODUCT_TYPE, commonsku, BAU_OFFLINE, BAU_ONLINE, TAX_RATE , row_number()over (partition by marketplace_sku order by 1) rw from zouk-wh.maplemonk.final_sku_master where lower(marketplace) like \'%myntra%\') where rw = 1 ) p on lower(fi.sku_id) = lower(p.skucode) UNION ALL SELECT NULL AS customer_id ,\'FLIPKART_FBF\' AS shop_name ,\'FLIPKART_FBF\' AS marektplace ,\'FLIPKART_FBF\' AS CHANNEL ,\'FLIPKART_FBF\' AS SOURCE ,cast(Product_Id as string) ORDER_ID ,cast(Product_Id as string) reference_code ,null AS PHONE ,null AS NAME ,null AS EMAIL ,cast(Order_Date as timestamp) AS SHIPPING_LAST_UPDATE_DATE ,sku_id as SKU ,Product_Id PRODUCT_ID ,name AS PRODUCT_NAME ,\'IN\' CURRENCY ,null AS city ,null AS State ,null AS order_status ,CAST(Order_Date AS DATE) AS Order_Date ,CAST(Order_Date AS datetime) ORDER_TIME ,cast(Gross_Units as int64) AS QUANTITY ,IFNULL(cast(Final_Sale_Amount as int64),0) - IFNULL(cast(0 as int64), 0) AS gross_sales_before_tax ,cast(0 as float64) AS DISCOUNT ,cast(0 as float64) TAX ,cast(0 as float64) as SHIPPING_PRICE ,cast(Final_Sale_Amount as float64) AS SELLING_PRICE ,null AS OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null AS FINAL_SHIPPING_STATUS ,Product_Id AS SALEORDERITEMCODE ,Product_Id AS SALES_ORDER_ITEM_ID ,null as AWB ,NULL AS payment_gateway ,null as payment_mode ,null COURIER ,null AS DISPATCH_DATE ,null AS delivered_date ,null as DELIVERED_STATUS , case when CAST(Return_Units AS INT64) > 0 then 1 else 0 end AS RETURN_FLAG ,CAST(Return_Units AS INT64) returned_quantity ,CAST(Return_Amount AS FLOAT64) AS returned_sales ,CAST(Cancellation_Units AS INT64) cancelled_quantity ,null AS NEW_CUSTOMER_FLAG ,NULL AS ACQUISITION_PRODUCT ,null days_in_shipment ,NULL AS ACQUISITION_DATE ,sku_id as sku_code ,name AS PRODUCT_NAME_FINAL ,p.category AS PRODUCT_CATEGORY ,sub_category AS PRODUCT_SUB_CATEGORY ,collection as COLLECTION ,print as PRINT ,PRODUCT_TYPE as PRODUCT_TYPE ,category_code category_code ,BAU_OFFLINE BAU_OFFLINE ,BAU_ONLINE BAU_ONLINE ,TAX_RATE as FINAL_TAX_RATE ,commonsku ,UPPER(Location_Id) AS warehouse ,null as pincode ,null source_pincode ,\'FLIPKART\' FINAL_MARKETPLACE ,\'MARKETPLACE\' MARKETPLACE_SEGMENT ,\'SALES\' TYPE_OF_SALE ,null discount_codes ,null as copoun_source from maplemonk.zouk_flipkart_fbf_orders fi left join (select * from (select marketplace_sku skucode, name, category, sub_category, category_code, collection, print, PRODUCT_TYPE, commonsku, BAU_OFFLINE, BAU_ONLINE, TAX_RATE , row_number()over (partition by marketplace_sku order by 1) rw from zouk-wh.maplemonk.final_sku_master where lower(marketplace) like \'%flipkart%\') where rw = 1 ) p on lower(fi.sku_id) = lower(p.skucode) ; CREATE OR REPLACE TABLE maplemonk.Final_customerID AS WITH new_phone_numbers AS ( SELECT phone, contact_num, 19700000000 + ROW_NUMBER() OVER (ORDER BY contact_num ASC) AS maple_monk_id FROM ( SELECT DISTINCT RIGHT(REGEXP_REPLACE(phone, \'[^a-zA-Z0-9]+\', \'\'), 10) AS contact_num, phone FROM maplemonk.zouk_sales_consolidated_intermediate ) a ), INT AS ( SELECT contact_num, email, COALESCE(maple_monk_id, id2) AS maple_monk_id FROM ( SELECT contact_num, email, maple_monk_id, 19800000000 + ROW_NUMBER() OVER (PARTITION BY maple_monk_id IS NULL ORDER BY email ASC) AS id2 FROM ( SELECT DISTINCT COALESCE(p.contact_num, RIGHT(REGEXP_REPLACE(e.contact_num, \'[^a-zA-Z0-9]+\', \'\'), 10)) AS contact_num, e.email, maple_monk_id FROM ( SELECT phone AS contact_num, email FROM maplemonk.zouk_sales_consolidated_intermediate ) e LEFT JOIN new_phone_numbers p ON p.contact_num = RIGHT(REGEXP_REPLACE(e.contact_num, \'[^a-zA-Z0-9]+\', \'\'), 10) ) a ) b ) SELECT contact_num, email, maple_monk_id FROM INT WHERE COALESCE(contact_num, email) IS NOT NULL; CREATE OR REPLACE TABLE maplemonk.zouk_sales_consolidated AS SELECT COALESCE(m.maple_monk_id_phone, d.maple_monk_id) AS customer_id_final, MIN(order_date) OVER (PARTITION BY COALESCE(m.maple_monk_id_phone, d.maple_monk_id)) AS acquisition_date, MIN(CASE WHEN LOWER(order_status) NOT IN (\'cancelled\') THEN order_date END) OVER (PARTITION BY COALESCE(m.maple_monk_id_phone, d.maple_monk_id)) AS first_complete_order_date, sum(ifnull(quantity, 0)) over(partition by reference_code order by 1) Order_Quantity, m.* FROM ( SELECT c.maple_monk_id AS maple_monk_id_phone, o.* FROM maplemonk.zouk_sales_consolidated_intermediate o LEFT JOIN ( SELECT * FROM ( SELECT contact_num AS phone, maple_monk_id, ROW_NUMBER() OVER (PARTITION BY contact_num ORDER BY maple_monk_id ASC) AS magic FROM maplemonk.Final_customerID ) WHERE magic = 1 ) c ON c.phone = RIGHT(REGEXP_REPLACE(o.phone, \'[^a-zA-Z0-9]+\', \'\'), 10) ) m LEFT JOIN ( SELECT DISTINCT maple_monk_id, email FROM maplemonk.Final_customerID WHERE contact_num IS NULL ) d ON d.email = m.email; ALTER TABLE maplemonk.zouk_sales_consolidated DROP COLUMN IF EXISTS new_customer_flag, DROP COLUMN IF EXISTS acquisition_product; ALTER TABLE maplemonk.zouk_sales_consolidated ADD COLUMN new_customer_flag STRING, ADD COLUMN new_customer_flag_month STRING, ADD COLUMN acquisition_product STRING, ADD COLUMN acquisition_channel STRING, ADD COLUMN acquisition_marketplace STRING; UPDATE maplemonk.zouk_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date THEN \'New\' WHEN Order_Date < first_complete_order_date OR first_complete_order_date IS NULL THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date THEN \'Repeat\' END AS Flag, row_number() over (partition by order_id, customer_id_final order by order_date) rw FROM maplemonk.zouk_sales_consolidated ) AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final and rw =1; UPDATE maplemonk.zouk_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL AND (LOWER(order_status) IS NULL OR LOWER(order_status) NOT IN (\'cancelled\')) THEN \'New\' WHEN new_customer_flag IS NULL AND LOWER(order_status) IN (\'cancelled\') THEN \'Yet to make completed order\' ELSE new_customer_flag END WHERE new_customer_flag IS NULL; UPDATE maplemonk.zouk_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, order_date, CASE WHEN DATE_TRUNC(order_date, MONTH) = DATE_TRUNC(first_complete_order_date, MONTH) THEN \'New\' WHEN DATE_TRUNC(order_date, MONTH) < DATE_TRUNC(first_complete_order_date, MONTH) OR acquisition_date IS NULL THEN \'Yet to make completed order\' WHEN DATE_TRUNC(order_date, MONTH) > DATE_TRUNC(first_complete_order_date, MONTH) THEN \'Repeat\' END AS flag, row_number() over (partition by order_id, customer_id_final order by order_date) rw FROM maplemonk.zouk_sales_consolidated ) AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final and rw=1; UPDATE maplemonk.zouk_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL AND (LOWER(order_status) IS NULL OR LOWER(order_status) NOT IN (\'cancelled\')) THEN \'New\' ELSE new_customer_flag_month END WHERE new_customer_flag_month IS NULL; CREATE OR REPLACE TEMPORARY TABLE temp_source_1 AS SELECT customer_id_final, source AS channel, shop_name AS marketplace FROM (SELECT * FROM( SELECT customer_id_final, order_date, source, shop_name, MIN(CASE WHEN LOWER(order_status) <> \'cancelled\' THEN order_date END) OVER (PARTITION BY customer_id_final) AS firstOrderDate, row_number() over (partition by customer_id_final order by ORDER_TIME) rw FROM maplemonk.zouk_sales_consolidated ) AS res where rw =1 ) WHERE order_date = firstOrderDate; UPDATE maplemonk.zouk_sales_consolidated AS a SET a.acquisition_channel = b.channel FROM temp_source_1 AS b WHERE a.customer_id_final = b.customer_id_final; UPDATE maplemonk.zouk_sales_consolidated AS a SET a.acquisition_marketplace = b.marketplace FROM temp_source_1 AS b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE temp_product_1 AS SELECT customer_id_final, product_name_final FROM ( SELECT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate, row_number() over (partition by customer_id_final order by ORDER_TIME asc, selling_price desc) rw FROM maplemonk.zouk_sales_consolidated )res WHERE order_date=firstorderdate and rw=1; UPDATE maplemonk.zouk_sales_consolidated AS A SET A.acquisition_product = B.product_name_final FROM ( SELECT * FROM temp_product_1 ) AS B WHERE A.customer_id_final = B.customer_id_final; Create or Replace Table maplemonk.zouk_sales_consolidated as select sc.* ,case when DATE_DIFF(current_date(), first_order_date, DAY) < 180 then \'NPD\' else \'NON-NPD\' end as NPD_BUCKET from maplemonk.zouk_sales_consolidated sc left join ( select coalesce(SKU_CODE,sku) as sku ,min(order_date) first_order_date from maplemonk.zouk_sales_consolidated group by 1 )sc1 on lower(sc1.sku) = lower(coalesce(sc.SKU_CODE,sc.sku)) ; CREATE TABLE IF NOT EXISTS maplemonk.zouk_easyecom_returns_intermediate ( ORDER_ID INT64, INVOICE_ID INT64, SUBORDER_ID STRING, REFERENCE_CODE STRING, CREDIT_NOTE_ID INT64, ORDER_DATE TIMESTAMP, INVOICE_DATE TIMESTAMP, RETURN_DATE TIMESTAMP, MANIFEST_DATE TIMESTAMP, IMPORT_DATE TIMESTAMP, LAST_UPDATE_DATE TIMESTAMP, COMPANY_PRODUCT_ID STRING, PRODUCTNAME STRING, PRODUCT_ID STRING, SKU STRING, MARKETPLACE STRING, MARKETPLACE_ID INT64, REPLACEMENT_ORDER INT64, RETURN_REASON STRING, RETURNED_QUANTITY FLOAT64, RETURN_AMOUNT_WITHOUT_TAX FLOAT64, RETURN_TAX FLOAT64, RETURN_SHIPPING_CHARGE FLOAT64, RETURN_MISC FLOAT64, TOTAL_RETURN_AMOUNT FLOAT64 ); CREATE OR REPLACE TABLE maplemonk.zouk_easyecom_returns_fact_items AS SELECT IFNULL(FE.Source, \'NA\') AS Marketing_CHANNEL, FR.* FROM maplemonk.zouk_easyecom_returns_intermediate FR LEFT JOIN ( SELECT DISTINCT REPLACE(reference_code, \'#\', \'\') AS REFERENCE_CODE, Source FROM maplemonk.zouk_sales_consolidated ) FE ON FR.REFERENCE_CODE = FE.REFERENCE_CODE; CREATE TABLE IF NOT EXISTS maplemonk.zouk_unicommerce_returns_intermediate ( MARKETPLACE STRING, SOURCE STRING, ORDER_ID STRING, REFERENCE_CODE STRING, PHONE STRING, NAME STRING, EMAIL STRING, ORDER_DATE TIMESTAMP, RETURN_DISPLAYCODE STRING, RETURN_STATUS STRING, INVENTORY_RECEIVED_DATE TIMESTAMP, RETURN_COMPLETE_DATE TIMESTAMP, RETURN_INVOICE_DISPLAY_CODE STRING, RETURN_COURIER STRING, RETURN_PROVIDER_SHIPPING_STATUS STRING, RETURN_TRACKING_NUMBER STRING, RETURN_TYPE STRING, sale_order_item_code STRING, ITEMSKU STRING, ITEM_NAME STRING, INVENTORY_TYPE STRING ); CREATE OR REPLACE TABLE maplemonk.zouk_unicommerce_returns_fact_items AS SELECT IFNULL(FE.channel, \'NA\') AS Marketing_CHANNEL, FE.quantity AS RETURNED_QUANTITY, FE.selling_price AS TOTAL_RETURN_AMOUNT, FE.tax AS RETURN_TAX, FE.selling_price - FE.tax AS RETURN_AMOUNT_WITHOUT_TAX, FR.* FROM (select * from maplemonk.zouk_unicommerce_returns_intermediate where return_display_code is not null) FR LEFT JOIN ( SELECT REPLACE(reference_code, \'#\', \'\') AS REFERENCE_CODE, SALEORDERITEMCODE, channel, Source, SUM(quantity) AS quantity, SUM(selling_price) AS Selling_price, SUM(tax) AS tax FROM maplemonk.zouk_sales_consolidated GROUP BY REFERENCE_CODE, SALEORDERITEMCODE, channel, Source ) FE ON FR.REFERENCE_CODE = FE.REFERENCE_CODE AND FR.sale_order_item_code = FE.SALEORDERITEMCODE; CREATE OR REPLACE TABLE maplemonk.zouk_returns_consolidated AS SELECT UPPER(er.MARKETPLACE) AS Marketplace, mm.FINAL_MARKETPLACE, cast(RETURN_DATE as datetime) return_Date, UPPER(Marketing_CHANNEL) AS Marketing_channel, SUM(RETURNED_QUANTITY) AS TOTAL_RETURNED_QUANTITY, SUM(TOTAL_RETURN_AMOUNT) AS TOTAL_RETURN_AMOUNT, SUM(RETURN_TAX) AS TOTAL_RETURN_TAX, SUM(RETURN_AMOUNT_WITHOUT_TAX) AS TOTAL_RETURN_AMOUNT_EXCL_TAX FROM maplemonk.zouk_easyecom_returns_fact_items er LEFT JOIN (select * from (Select * , row_number() over (partition by lower(marketplace) order by 1) rw from maplemonk.marketplace_mapping ) where rw =1 ) mm ON lower(er.marketplace) = lower(mm.marketplace) GROUP BY er.Marketplace, mm.final_marketplace, RETURN_DATE, Marketing_channel UNION ALL SELECT UPPER(fr.MARKETPLACE) AS Marketplace, mm.FINAL_MARKETPLACE, cast(RETURN_COMPLETE_DATE as datetime) AS RETURN_DATE, UPPER(Marketing_CHANNEL) AS Marketing_channel, SUM(RETURNED_QUANTITY) AS TOTAL_RETURNED_QUANTITY, SUM(TOTAL_RETURN_AMOUNT) AS TOTAL_RETURN_AMOUNT, SUM(RETURN_TAX) AS TOTAL_RETURN_TAX, SUM(RETURN_AMOUNT_WITHOUT_TAX) AS TOTAL_RETURN_AMOUNT_EXCL_TAX FROM maplemonk.zouk_unicommerce_returns_fact_items fr LEFT JOIN (select * from (Select * , row_number() over (partition by lower(marketplace) order by 1) rw from maplemonk.marketplace_mapping ) where rw =1 ) mm ON lower(fr.marketplace) = lower(mm.marketplace) GROUP BY fr.Marketplace, mm.FINAL_MARKETPLACE, RETURN_COMPLETE_DATE, Marketing_channel;",
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
            