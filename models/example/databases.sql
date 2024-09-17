{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE table if not exists ttk_db.Maplemonk.ttk_db_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, product_category varchar, product_sub_category varchar) ; create table if not exists ttk_db.Maplemonk.ttk_db_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar, awb varchar,Marketplace_LineItem_ID varchar, reference_code varchar,LAST_UPDATE_DATE date,PAYMENT_MODE varchar,COURIER varchar,MANIFEST_DATE date, DELIVERED_DATE date,mapped_product_name varchar,mapped_category varchar, mapped_sub_category varchar, warehouse_name varchar) ; create table if not exists ttk_db.Maplemonk.ttk_db_UNICOMMERCE_FACT_ITEMS ( order_id varchar ,order_Date date ,reference_code varchar ,name varchar ,email varchar ,city varchar ,state varchar ,phone varchar ,saleorderitemcode varchar ,sales_order_item_id varchar ,shippingpackagecode varchar ,SHIPPINGPACKAGESTATUS varchar ,shipping_status varchar ,order_status varchar ,Courier varchar ,Dispatch_Date date ,Delivered_date date ,Return_flag int ,Return_quantity int ,suborder_quantity int ,cancelled_quantity int ,selling_price float ,shipping_price float ,tax float ,discount float ,shipping_last_update_date date ,days_in_shipment float ,awb varchar ,marketplace varchar ,payment_method varchar ,PAYMENT_MODE varchar ,PRODUCT_ID varchar ,SKU varchar ,SKU_CODE varchar ,currency varchar ,NEW_CUSTOMER_FLAG varchar ,product_name varchar ,mapped_product_name varchar ,product_name_final varchar ,mapped_category varchar ,product_category varchar ,mapped_sub_category varchar ,product_sub_category varchar ,warehouse_name varchar) ; create table if not exists ttk_db.Maplemonk.ttk_db_SHOPIFY_FACT_ITEMS( SHOPIFY_CUSTOMER_ID_FINAL INT ,SHOPIFY_ACQUISITION_DATE DATE ,SHOPIFY_FIRST_COMPLETE_ORDER_DATE DATE ,MAPLE_MONK_ID_PHONE INT ,SHOP_NAME VARCHAR ,MARKETPLACE VARCHAR ,ORDER_ID VARCHAR ,ORDER_NAME VARCHAR ,CUSTOMER_ID INT ,NAME VARCHAR ,EMAIL VARCHAR ,PHONE VARCHAR ,TAGS VARCHAR ,LINE_ITEM_ID VARIANT ,SKU VARCHAR ,PRODUCT_ID VARCHAR ,CURRENCY VARCHAR ,IS_REFUND INT ,SHIPPING_CITY VARCHAR ,SHIPPING_STATE VARCHAR ,CITY VARCHAR ,STATE VARCHAR ,PRODUCT_NAME VARCHAR ,CATEGORY VARCHAR ,ORDER_STATUS VARCHAR ,ORDER_TIMESTAMP TIMESTAMP_NTZ(9) ,LINE_ITEM_SALES FLOAT ,QUANTITY FLOAT ,REFUND_QUANTITY INT ,REFUND_VALUE FLOAT ,TAX FLOAT ,TAX_RATE FLOAT ,DISCOUNT FLOAT ,DISCOUNT_BEFORE_TAX FLOAT ,GROSS_SALES_AFTER_TAX FLOAT ,GROSS_SALES_BEFORE_TAX FLOAT ,NET_SALES_BEFORE_TAX FLOAT ,SHIPPING_PRICE FLOAT ,TOTAL_SALES FLOAT ,SOURCE VARCHAR ,MOMENTS_COUNT INT ,DAYSTOCONVERT INT ,SHOPIFYQL_FIRSTVISIT_UTM_SOURCE VARCHAR ,SHOPIFYQL_MAPPED_SOURCE VARCHAR ,SHOPIFYQL_MAPPED_CHANNEL VARCHAR ,SHOPIFYQL_LAST_MOMENT_UTM_SOURCE VARCHAR ,SHOPIFYQL_LAST_VISIT_NON_UTM_SOURCE VARCHAR ,SHOPIFYQL_FIRSTVISIT_UTM_MEDIUM VARCHAR ,SHOPIFYQL_LAST_MOMENT_UTM_MEDIUM VARCHAR ,FINAL_UTM_CHANNEL VARCHAR ,FINAL_UTM_CAMPAIGN VARCHAR ,FINAL_UTM_SOURCE VARCHAR ,REFERRER_NAME VARCHAR ,GOKWIK_MAPPED_SOURCE VARCHAR ,GOKWIK_MAPPED_CHANNEL VARCHAR ,REFUND_DETAILS ARRAY ,AWB VARCHAR ,COURIER VARCHAR ,SHIPPING_STATUS VARCHAR ,SHIPPING_STATUS_UPDATE_DATE VARCHAR ,TRACKING_URL VARCHAR ,SHIPPING_CREATED_AT VARCHAR ,GATEWAY VARCHAR ,PAYMENT_MODE VARCHAR ,SKU_CODE VARCHAR ,COMMONSKUID VARCHAR ,PRODUCT_NAME_FINAL VARCHAR ,PRODUCT_CATEGORY VARCHAR ,PRODUCT_SUB_CATEGORY VARCHAR ,SHOPIFY_NEW_CUSTOMER_FLAG VARCHAR ,SHOPIFY_NEW_CUSTOMER_FLAG_MONTH VARCHAR ,SHOPIFY_ACQUISITION_PRODUCT VARCHAR ,SHOPIFY_ACQUISITION_CHANNEL VARCHAR ,SHOPIFY_ACQUISITION_SOURCE VARCHAR ,SHIPPING_TAX FLOAT ,SHIP_PROMOTION_DISCOUNT FLOAT ,GIFT_WRAP_PRICE FLOAT ,GIFT_WRAP_TAX FLOAT) ; CREATE or replace TABLE ttk_db.Maplemonk.shipment_status_mapping (SHIPPING_STATUS VARCHAR(16777216),Mapped_Status VARCHAR(16777216)); Insert into ttk_db.Maplemonk.shipment_status_mapping values (\'DELIVERED\',\'Delivered\') ,(\'CANCELLED\',\'Cancelled\') ,(\'DELIVERED TO ORIGIN\',\'Returned\') ,(\'SHIPMENT CREATED\',\'Shipment Created\') ,(\'RETURNED\',\'Returned\') ,(\'IN TRANSIT\',\'In Transit\') ,(\'DELIVERY DELAYED\',\'In Transit\') ,(\'DELIVERED TO ORIGIN\',\'RTO\') ,(\'CONFIRMED\',\'Open\') ,(\'IN_TRANSIT\',\'In Transit\') ,(\'RTO DELIVERED\',\'RTO\') ,(\'CANCELED\',\'Cancelled\') ,(\'RTO IN TRANSIT\',\'RTO\') ,(\'REACHED AT DESTINATION HUB\',\'In Transit\') ,(\'PICKED UP\',\'In Transit\') ,(\'UNDELIVERED-2ND ATTEMPT\',\'In Transit\') ,(\'RTO_OFD\',\'RTO\') ,(\'OUT FOR DELIVERY\',\'Out for delivery\') ,(\'RTO INITIATED\',\'RTO\') ,(\'PICKUP EXCEPTION\',\'Pickup Error\') ,(\'CANCELLATION REQUESTED\',\'Pickup Error\') ,(\'READY TO SHIP\',\'Ready to Ship\') ,(\'OUT_FOR_DELIVERY\',\'Out for delivery\') ,(\'SHIPPED\',\'In Transit\') ,(\'PICKUP RESCHEDULED\',\'Pickup Error\') ,(\'UNDELIVERED-3RD ATTEMPT\',\'In Transit\') ,(\'MISROUTED\',\'Misrouted\') ,(\'PICKUP SCHEDULED\',\'Pickup Error\') ,(\'UNDELIVERED-1ST ATTEMPT\',\'In Transit\') ,(\'Not Traceble\',\'Pickup Error\') ,(\'UNDELIVERED\',\'In Transit\') ,(\'LOST\',\'Lost\') ,(\'IN TRANSIT-EN-ROUTE\',\'In Transit\') ,(\'Indian Speed Post\',\'Indian Speed Post\') ,(\'Exception\',\'Exception\') ,(\'Shipment Booked\',\'Pickup Error\') ,(\'Assigned\',\'Assigned\') ,(\'Pending\',\'Pending\') ,(\'Printed\',\'Printed\') ,(\'IN TRANSIT-AT DESTINATION HUB\',\'In Transit\') ,(\'Upcoming\',\'Order Yet To sync\') ,(\'RTO-In Transit\',\'RTO\') ,(\'PICKUP ERROR\',\'Pickup Error\') ,(\'RTO-Out for Delivery\',\'RTO\') ,(\'RTO-Delivered\',\'RTO\') ,(\'OUT FOR PICKUP\',\'Pickup Error\') ,(\'RTO_NDR\',\'RTO\') ,(\'RTO-Exception\',\'Exception\') ,(\'Delayed\',\'In Transit\') ,(\'IN TRANSIT-AT SOURCE HUB\',\'In Transit\') ,(\'RTS\',\'Underprocess\') ,(\'Manifested\',\'Pickup Error\') ,(\'Dispatched\',\'In Transit\') ,(\'RTO\',\'RTO\') ,(\'Not Picked\',\'Pickup Error\') ,(\'DAMAGED\',\'Damaged\') ,(\'Closed\',\'Cancelled\') ,(\'Open\',\'Open\') ,(\'On Hold\',\'On Hold\') ,(\'Shipped - Returned to Seller\',\'RTO\') ,(\'Shipped - Delivered to Buyer\',\'Delivered\') ,(\'Shipped - Picked Up\',\'In Transit\') ,(\'Shipped - Rejected by Buyer\',\'RTO\') ,(\'Shopify_Processed\',\'Shopify_Processed\') ,(\'Ready to dispatch\',\'Ready to Dispatch\') ,(\'RTO Undelivered\',\'RTO\') ,(\'Shipment Lost\',\'Lost\') ,(\'Shipment Error\',\'Shipment Error\') ,(\'RTO In-Transit\',\'RTO\') ,(\'Dispatch / RTO\',\'RTO\') ,(\'Pending Pick-up\',\'Pending\') ,(\'Dispatch/Intransit\',\'In Transit\') ,(\'Dispatch /Lost and Damange\',\'Lost\') ,(\'Dispatch /Undelivered\',\'Undelivered\') ,(\'Non serviceable\',\'Non Serviceable\') ,(\'Not serviceable\',\'Non Serviceable\') ,(\'Dispatch/Delivered\',\'Delivered\') ,(\'Refund\',\'Refund\') ,(\'F&P\',\'F&P\') ,(\'SHIPPED - RETURNING TO SELLER\',\'RTO\') ,(\'SHIPPED - LOST IN TRANSIT\',\'Lost\') ,(\'SHIPPED - DAMAGED\',\'Damaged\') ,(\'SHIPPED - OUT FOR DELIVERY\',\'Out for delivery\'); create or replace table ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate_pre as select b.customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.shop_name) as marketplace ,Upper(b.FINAL_UTM_CHANNEL) AS CHANNEL ,Upper(b.FINAL_UTM_SOURCE) AS SOURCE ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(wi.LAST_UPDATED_TIME, wi2.LAST_UPDATED_TIME,c.shipping_last_update_date::datetime, b.shipping_status_update_date, d.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.order_timestamp ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(wi.status, wi2.status, c.shipping_status, b.shipping_status, d.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(override.status,case when lower(b.ORDER_STATUS) like \'%cancel%\' or lower(c.ORDER_STATUS) like \'%cancel%\' or lower(c.shipping_status) like \'%cancel%\' then \'CANCELLED\' else coalesce(shipmap.final_shipping_status,wi.status, wi2.status,c.shipping_status, b.shipping_status, d.shipping_status) end)) FINAL_SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,d.sales_order_item_id as SALES_ORDER_ITEM_ID ,coalesce(wi.awb,wi2.awb,c.awb,b.awb,d.awb) AWB ,UPPER(b.GATEWAY) PAYMENT_GATEWAY ,upper(coalesce(wi.payment_mode,wi2.payment_mode, c.payment_mode,d.payment_mode)) Payment_Mode ,Upper(coalesce(wi.courier,wi2.courier,c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(wi.manifest_time,wi2.manifest_time,c.manifest_date,b.Shipping_created_at,d.dispatch_date) AS DISPATCH_DATE ,coalesce(wi.delivered_time,wi2.delivered_time,c.delivered_date,d.delivered_date,case when b.shipping_status like \'delivered\' then b.shipping_status_update_date end) AS DELIVERED_DATE ,case when lower(coalesce(override.status, shipmap.final_shipping_status,wi.status,wi2.status,c.shipping_status,b.shipping_status, d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') and lower(c.shipping_status) not in (\'cancelled\') and lower(coalesce(override.status, wi.status, wi2.status)) not like \'%cancel%\' then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') and lower(c.shipping_status) not in (\'cancelled\') and lower(coalesce(override.status, wi.status, wi2.status)) not like \'%cancel%\' then ifnull(refund_quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') and lower(c.shipping_status) not in (\'cancelled\') and lower(coalesce(override.status, wi.status, wi2.status)) not like \'%cancel%\' then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') or lower(c.shipping_status) like \'%cancel%\' or lower(coalesce(override.status, wi.status, wi2.status)) like \'%cancel%\' then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(coalesce(override.status,shipmap.final_shipping_status,wi.status,wi2.status, c.shipping_status, b.shipping_status,d.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(coalesce(wi.last_updated_time,c.shipping_last_update_date::datetime, b.shipping_status_update_date, d.shipping_last_update_date::datetime))) when lower(coalesce(override.status,shipmap.final_shipping_status,wi.status,wi2.status, c.shipping_status, b.shipping_status,d.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,b.brand ,b.product_for ,upper(c.warehouse_name) WAREHOUSE ,coalesce(b.pincode, c.pincode) pincode ,c.shipping_status easyecom_Status ,wi.status wareiq_sku_status ,wi2.status wareiq_awb_status from ttk_db.Maplemonk.ttk_db_SHOPIFY_FACT_ITEMS b left join (select * from ( select * ,row_number()over(partition by reference_code, Marketplace_LineItem_ID, suborder_id order by last_update_date desc) rw from ttk_db.Maplemonk.ttk_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%shopify%\') ) c on lower(replace(b.order_name,\'#\',\'\')) = lower(c.reference_code) and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,PAYMENT_MODE ,email ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from ttk_db.Maplemonk.ttk_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=split_part(d.saleorderitemcode,\'-\',0) left join (select * from (select * ,row_number() over (partition by lower(order_id), lower(sku) order by last_updated_time desc) tw from ttk_db.maplemonk.ttk_db_wareiq_fact_items ) where tw =1 ) wi on lower(replace(b.order_name,\'#\',\'\'))=lower(wi.order_id) and lower(c.easyecom_sku) = lower(WI.SKU) left join (select * from (select * ,row_number() over (partition by lower(order_id), awb order by last_updated_time desc) zw from ttk_db.maplemonk.ttk_db_wareiq_fact_items ) where zw =1 ) wi2 on lower(replace(b.order_name,\'#\',\'\'))=lower(wi2.order_id) and lower(b.awb) = lower(WI2.awb) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from ttk_db.Maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(wi.status,wi2.status,c.shipping_status,b.shipping_status,d.shipping_status,b.ORDER_STATUS)) = lower(ShipMap.shipping_status) left join (select * from (select * , row_number() over (partition by ifnull(awb,\'1\'),lower(SKU),lower(status), lower(order_id) order by 1) rw from ttk_db.maplemonk.orders_status_override ) where rw =1 ) Override on replace(b.order_name,\'#\',\'\') = replace(override.order_id, \'#\',\'\') and lower(b.sku) = lower(override.sku) union all select Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,\'AMAZON\' AS SOURCE ,afi.ORDER_ID ,afi.ORDER_ID reference_code ,Null as PHONE ,NAME ,coalesce(EEFI.EMAIL,UFI.EMAIL,AFI.EMAIL) AS EMAIL ,coalesce(EEFI.shipping_last_update_date::datetime, UFI.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(afi.STATE) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP::date AS Order_Date ,afi.order_timestamp ,afi.QUANTITY ,ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,afi.TAX ,afi.SHIPPING_PRICE ,TOTAL_SALES AS SELLING_PRICE ,upper(coalesce(EEFI.order_status,UFI.order_status)) as OMS_order_status ,upper(coalesce(EEFI.shipping_status,UFI.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) FINAL_SHIPPING_STATUS ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALEORDERITEMCODE ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALES_ORDER_ITEM_ID ,coalesce(EEFI.awb,UFI.awb) AWB ,NULL Payment_Gateway ,upper(coalesce(EEFI.payment_mode,UFI.payment_mode)) Payment_Mode ,Upper(coalesce(EEFI.Courier,UFI.courier)) AS COURIER ,coalesce(EEFI.manifest_date,UFI.dispatch_date) AS DISPATCH_DATE ,coalesce(EEFI.delivered_date,UFI.delivered_date) AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,ufi.shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,afi.IS_REFUND AS RETURN_FLAG ,case when afi.is_refund = 1 then quantity::int end returned_quantity ,case when afi.is_refund = 1 then total_sales end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce(shipmap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(afi.ORDER_TIMESTAMP),date(coalesce(ufi.shipping_last_update_date::datetime, eefi.shipping_last_update_date::datetime))) when lower(coalesce( shipmap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(afi.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,coalesce(afi.SKU,ufi.PRODUCT_ID,eefi.SKU) as SKU_CODE ,UPPER(AFI.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,null as BRAND ,null as PRODUCT_FOR ,upper(EEFI.warehouse_name) WAREHOUSE ,EEFI.pincode ,eefi.shipping_status easyecom_Status ,null wareiq_sku_status ,null wareiq_awb_status from ttk_db.Maplemonk.ttk_db_AMAZON_FACT_ITEMS AFI left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from ttk_db.Maplemonk.ttk_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\') ) EEFI on AFI.Order_id = EEFI.reference_code and AFI.PRODUCT_ID = EEFI.sku left join (select * from (select order_id ,city ,state ,product_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,payment_mode ,email ,row_number() over (partition by order_id, product_id order by shipping_last_update_date desc) rw from ttk_db.Maplemonk.ttk_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%amazon%\')) where rw=1 ) UFI on AFI.order_id = UFI.order_id and AFI.SKU = UFI.PRODUCT_ID left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from ttk_db.Maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(EEFI.shipping_status,UFI.shipping_status,afi.ORDER_STATUS)) = lower(ShipMap.shipping_status) union all select Null as customer_id ,case when upper(b.SHOP_NAME) like \'%MAGENTO%\' then \'MAGENTO_SKORE\' else upper(b.SHOP_NAME) end as SHOP_NAME ,case when upper(marketplace) like \'%MAGENTO%\' then \'MAGENTO_SKORE\' else upper(marketplace) end marketplace ,case when upper(marketplace) like \'%MAGENTO%\' then coalesce(GA.CHANNEL,\'MAGENTO_SKORE\') else upper(marketplace) end AS CHANNEL ,case when upper(marketplace) like \'%MAGENTO%\' then coalesce(GA.FINAL_SOURCE,\'MAGENTO_SKORE\') else upper(marketplace) end AS SOURCE ,b.ORDER_ID ,reference_code ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,coalesce(wi.last_updated_time,wi2.last_updated_time,b.shipping_last_update_date) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.easyecom_sku PRODUCT_ID ,upper(PRODUCTNAME) AS PRODUCT_NAME ,CURRENCY ,upper(CITY) City ,upper(STATE) AS State ,upper(ORDER_STATUS) as Order_Status ,ORDER_DATE::date AS Order_Date ,ORDER_DATE ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0)-ifnull(tax,0)+ifnull(DISCOUNT,0) AS GROSS_SALES_BEFORE_TAX ,case when SUBORDER_QUANTITY*SUBORDER_MRP - (SELLING_PRICE - SHIPPING_PRICE) < 0 then 0 else SUBORDER_QUANTITY*SUBORDER_MRP - (SELLING_PRICE -SHIPPING_PRICE) end AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_Order_Status ,upper(coalesce(wi.status,wi2.status,b.Shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(override.status, case when lower(oms_order_status) like \'%cancel%\' or lower(coalesce(wi.status,wi2.status,b.Shipping_status)) like \'%cancel%\' then \'CANCELLED\' else coalesce(shipmap.final_shipping_status,wi.status, wi2.status,b.shipping_status) end)) FINAL_SHIPPING_STATUS ,Marketplace_LineItem_ID as SALEORDERITEMCODE ,suborder_id as SALES_ORDER_ITEM_ID ,coalesce(wi.AWB,wi2.awb,b.awb) AWB ,NULL Payment_Gateway ,coalesce(wi.payment_mode,wi2.payment_mode, b.payment_mode) Payment_Mode ,UPPER(coalesce(wi.COURIER,wi2.COURIER,b.courier)) COURIER ,coalesce(wi.MANIFEST_TIME,wi2.MANIFEST_TIME,b.MANIFEST_DATE) as DISPATCH_DATE ,coalesce(wi.DELIVERED_TIME,wi2.DELIVERED_TIME, b.DELIVERED_DATE) DELIVERED_DATE ,case when lower(coalesce(override.status, ShipMap.shipping_status,wi.status,wi2.status,b.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then suborder_quantity::int end returned_quantity ,case when RETURN_FLAG = 1 and lower(order_status) not in (\'cancelled\') and lower(coalesce(override.status,wi.status,wi2.status)) not like \'%cancel%\' then ifnull(is_refund,0) end returned_sales ,case when is_refund = 0 and (lower(order_status) in (\'cancelled\') or lower(coalesce(override.status, wi.status,wi2.status)) like \'%cancel%\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,Days_in_shipment AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,b.SKU as SKU_CODE ,upper(mapped_product_name) as PRODUCT_NAME_FINAL ,upper(mapped_category) as PRODUCT_CATEGORY ,upper(mapped_sub_category) as PRODUCT_SUB_CATEGORY ,b.mapped_brand ,b.product_for ,upper(WAREHOUSE_NAME) WAREHOUSE ,b.pincode ,b.shipping_status easyecom_status ,wi.status wareiq_sku_status ,wi2.status wareiq_awb_status from ttk_db.Maplemonk.ttk_db_EasyEcom_FACT_ITEMS b left join (select * from (select * ,row_number() over (partition by transactionid, shop_name order by 1) rw from ttk_db.Maplemonk.ttk_db_GA_ORDER_BY_SOURCE_CONSOLIDATED ) where rw =1 ) GA on b.reference_code = GA.transactionid left join (select * from (select * ,row_number() over (partition by lower(order_id), lower(sku) order by last_updated_time desc) tw from ttk_db.maplemonk.ttk_db_wareiq_fact_items ) where tw =1 ) wi on lower(replace(b.reference_code,\'#\',\'\'))=lower(wi.order_id) and lower(b.EasyEcom_SKU) = lower(wi.sku) left join (select * from (select * ,row_number() over (partition by lower(order_id), awb order by last_updated_time desc) zw from ttk_db.maplemonk.ttk_db_wareiq_fact_items ) where zw =1 ) wi2 on lower(replace(b.reference_code,\'#\',\'\'))=lower(wi2.order_id) and lower(b.awb) = lower(wi2.awb) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from ttk_db.Maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(wi.status,wi2.status,b.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) left join (select * from (select * , row_number() over (partition by ifnull(awb,\'1\'),lower(SKU),lower(status), lower(order_id) order by 1) rw from ttk_db.maplemonk.orders_status_override ) where rw =1 ) Override on replace(b.reference_code,\'#\',\'\') = replace(override.order_id, \'#\',\'\') and lower(b.easyecom_sku) = lower(override.sku) where lower(marketplace) not like (\'%shopify%\' ) ; create or replace table ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate as select CUSTOMER_ID, SHOP_NAME, a.MARKETPLACE, CHANNEL, SOURCE, ORDER_ID, REFERENCE_CODE, PHONE, a.NAME, EMAIL, SHIPPING_LAST_UPDATE_DATE, a.SKU, PRODUCT_ID, PRODUCT_NAME, CURRENCY, a.CITY, a.STATE, ORDER_STATUS, ORDER_DATE, ORDER_TIMESTAMP, div0(QUANTITY, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) QUANTITY, div0(GROSS_SALES_BEFORE_TAX, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) GROSS_SALES_BEFORE_TAX, div0(DISCOUNT, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) DISCOUNT, div0(TAX, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) TAX, div0(SHIPPING_PRICE, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) SHIPPING_PRICE, div0(a.SELLING_PRICE, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) SELLING_PRICE, OMS_ORDER_STATUS, SHIPPING_STATUS, FINAL_SHIPPING_STATUS, SALEORDERITEMCODE, SALES_ORDER_ITEM_ID, AWB, PAYMENT_GATEWAY, PAYMENT_MODE, COURIER, DISPATCH_DATE, DELIVERED_DATE, DELIVERED_STATUS, RETURN_FLAG, div0(RETURNED_QUANTITY, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) RETURNED_QUANTITY, div0(RETURNED_SALES, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) RETURNED_SALES, div0(CANCELLED_QUANTITY, count(1) over (partition by reference_code, ifnull(a.sku,\'1\'),SALEORDERITEMCODE, a.marketplace order by 1)) cancelled_quantity, NEW_CUSTOMER_FLAG, ACQUISITION_PRODUCT, DAYS_IN_SHIPMENT, ACQUSITION_DATE, SKU_CODE, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, PRODUCT_SUB_CATEGORY, BRAND, PRODUCT_FOR, WAREHOUSE, a.PINCODE, EASYECOM_STATUS, WAREIQ_SKU_STATUS, WAREIQ_AWB_STATUS, upper(b.city) city_mapped, upper(b.state) state_mapped, coalesce(e.skucode_child, a.sku) skucode_child, coalesce(e.qty*a.quantity, a.quantity) child_quantity, coalesce(e.product_name_child, product_name_final) product_name_child, coalesce(e.product_category_child, PRODUCT_CATEGORY) product_category_child, coalesce(e.product_sub_Category_child, PRODUCT_SUB_CATEGORY) product_sub_Category_child, round(f.selling_price,2) MSP from ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate_pre a left join (select * from (select pincode, city, state, row_number() over (partition by pincode order by 1) rw from ttk_db.Maplemonk.pincode_city_mapping )where rw = 1 ) b on a.pincode = b.pincode left join ( select c.skucode, c.skucode_child, c.qty, d.name product_name_child, d.category product_category_child, d.sub_Category product_sub_category_child from (select skucode, skucode_child, qty from (select skucode, skucode_child, qty, row_number() over (partition by skucode, skucode_child order by 1) rw from ttk_db.maplemonk.sku_mapping_parent_child ) where rw = 1) c left join (select * from (select SKU skucode, UPPER(PRODUCTNAME) name, upper(category) CATEGORY, UPPER(\"Product Type\") sub_category, upper(brand) Brand, upper(\"For\") PRODUCT_FOR, row_number() over (partition by SKU order by 1) rw from ttk_db.Maplemonk.sku_master) where rw = 1 ) d on lower(c.skucode_child) = lower(d.skucode) ) e on lower(a.sku) = lower(e.skucode) left join ( select sku, marketplace, selling_price from (select *, row_number() over (partition by sku, marketplace order by quantity desc) rw from (select sku, selling_price,marketplace, sum(quantity) quantity from ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate_pre group by 1,2,3 ) )where rw = 1 ) f on f.sku = a.sku and f.marketplace = a.marketplace ; create or replace table ttk_db.Maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table ttk_db.Maplemonk.ttk_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, case when lower(final_shipping_status) like \'delivered\' then \'DELIVERED\' when lower(final_shipping_status) like any (\'%transit\', \'pending\') then \'IN TRANSIT\' when (lower(final_shipping_status) like any (\'rto\', \'pending-rt\', \'dispatched-rt\', \'%transit-rt\', \'%lost%\') or lower(shipping_status) like any (\'rto\', \'pending-rt\', \'dispatched-rt\', \'%transit-rt\', \'%lost%\')) and AWB is not null then \'CANCELLED BEFORE DELIVERY\' when (lower(final_shipping_status) like \'%cancel%\' or lower(oms_order_status) like \'%return%\') and (AWB is null or lower(shipping_status) like any (\'not shipped\', \'confirmed\', \'%cancel%\')) then \'CANCELLED BEFORE DISPATCH\' when lower(final_shipping_status) like \'%test%\' then \'CANCELLED BEFORE DISPATCH\' else \'OTHERS\' end AS Final_Status, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from ttk_db.Maplemonk.ttk_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from ttk_db.Maplemonk.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from ttk_db.Maplemonk.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE ttk_db.Maplemonk.ttk_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM ttk_db.Maplemonk.ttk_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM ttk_db.Maplemonk.ttk_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE ttk_db.Maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM ttk_db.Maplemonk.ttk_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM ttk_db.Maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM ttk_db.Maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE ttk_db.Maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM ttk_db.Maplemonk.ttk_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE ttk_db.Maplemonk.ttk_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM ttk_db.Maplemonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; CREATE table if not exists ttk_db.Maplemonk.ttk_db_easyecom_returns_intermediate (ORDER_ID NUMBER,INVOICE_ID NUMBER,SUBORDER_ID VARIANT,REFERENCE_CODE VARCHAR,CREDIT_NOTE_ID NUMBER,ORDER_DATE TIMESTAMP_NTZ,INVOICE_DATE TIMESTAMP_NTZ,RETURN_DATE TIMESTAMP_NTZ,MANIFEST_DATE TIMESTAMP_NTZ,IMPORT_DATE TIMESTAMP_NTZ,LAST_UPDATE_DATE TIMESTAMP_NTZ,COMPANY_PRODUCT_ID VARIANT,PRODUCTNAME VARCHAR,PRODUCT_ID VARIANT,SKU VARCHAR,MARKETPLACE VARCHAR,MARKETPLACE_ID NUMBER,REPLACEMENT_ORDER NUMBER,RETURN_REASON VARCHAR,RETURNED_QUANTITY FLOAT,RETURN_AMOUNT_WITHOUT_TAX FLOAT,RETURN_TAX FLOAT,RETURN_SHIPPING_CHARGE FLOAT,RETURN_MISC FLOAT,TOTAL_RETURN_AMOUNT FLOAT) ; Create or replace table ttk_db.Maplemonk.ttk_db_easyecom_returns_fact_items as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from ttk_db.Maplemonk.ttk_db_easyecom_returns_intermediate FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from ttk_db.Maplemonk.ttk_db_sales_consolidated) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table ttk_db.Maplemonk.ttk_db_RETURNS_CONSOLIDATED as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from ttk_db.Maplemonk.ttk_db_easyecom_returns_fact_items group by 1,2,3 order by 2 desc;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ttk_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            