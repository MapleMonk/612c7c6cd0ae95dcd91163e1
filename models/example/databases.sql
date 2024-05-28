{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE table if not exists charak_db.MAPLEMONK.charak_db_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, product_category varchar, product_sub_category varchar) ; create table if not exists charak_db.MAPLEMONK.charak_db_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar, awb varchar,Marketplace_LineItem_ID varchar, reference_code varchar,LAST_UPDATE_DATE date,PAYMENT_MODE varchar,COURIER varchar,MANIFEST_DATE date, DELIVERED_DATE date,mapped_product_name varchar,mapped_category varchar, mapped_sub_category varchar, warehouse_name varchar) ; create table if not exists charak_db.MAPLEMONK.charak_db_UNICOMMERCE_FACT_ITEMS ( order_id varchar ,order_Date date ,reference_code varchar ,name varchar ,email varchar ,city varchar ,state varchar ,phone varchar ,saleorderitemcode varchar ,sales_order_item_id varchar ,shippingpackagecode varchar ,SHIPPINGPACKAGESTATUS varchar ,shipping_status varchar ,order_status varchar ,Courier varchar ,Dispatch_Date date ,Delivered_date date ,Return_flag int ,Return_quantity int ,suborder_quantity int ,cancelled_quantity int ,selling_price float ,shipping_price float ,tax float ,discount float ,shipping_last_update_date date ,days_in_shipment float ,awb varchar ,marketplace varchar ,payment_method varchar ,PAYMENT_MODE varchar ,PRODUCT_ID varchar ,SKU varchar ,SKU_CODE varchar ,currency varchar ,NEW_CUSTOMER_FLAG varchar ,product_name varchar ,mapped_product_name varchar ,product_name_final varchar ,mapped_category varchar ,product_category varchar ,mapped_sub_category varchar ,product_sub_category varchar ,warehouse_name varchar) ; create table if not exists charak_db.MAPLEMONK.charak_db_SHOPIFY_FACT_ITEMS( SHOPIFY_CUSTOMER_ID_FINAL INT ,SHOPIFY_ACQUISITION_DATE DATE ,SHOPIFY_FIRST_COMPLETE_ORDER_DATE DATE ,MAPLE_MONK_ID_PHONE INT ,SHOP_NAME VARCHAR ,MARKETPLACE VARCHAR ,ORDER_ID VARCHAR ,ORDER_NAME VARCHAR ,CUSTOMER_ID INT ,NAME VARCHAR ,EMAIL VARCHAR ,PHONE VARCHAR ,TAGS VARCHAR ,LINE_ITEM_ID VARIANT ,SKU VARCHAR ,PRODUCT_ID VARCHAR ,CURRENCY VARCHAR ,IS_REFUND INT ,SHIPPING_CITY VARCHAR ,SHIPPING_STATE VARCHAR ,CITY VARCHAR ,STATE VARCHAR ,PRODUCT_NAME VARCHAR ,CATEGORY VARCHAR ,ORDER_STATUS VARCHAR ,ORDER_TIMESTAMP TIMESTAMP_NTZ(9) ,LINE_ITEM_SALES FLOAT ,QUANTITY FLOAT ,REFUND_QUANTITY INT ,REFUND_VALUE FLOAT ,TAX FLOAT ,TAX_RATE FLOAT ,DISCOUNT FLOAT ,DISCOUNT_BEFORE_TAX FLOAT ,GROSS_SALES_AFTER_TAX FLOAT ,GROSS_SALES_BEFORE_TAX FLOAT ,NET_SALES_BEFORE_TAX FLOAT ,SHIPPING_PRICE FLOAT ,TOTAL_SALES FLOAT ,SOURCE VARCHAR ,MOMENTS_COUNT INT ,DAYSTOCONVERT INT ,SHOPIFYQL_FIRSTVISIT_UTM_SOURCE VARCHAR ,SHOPIFYQL_MAPPED_SOURCE VARCHAR ,SHOPIFYQL_MAPPED_CHANNEL VARCHAR ,SHOPIFYQL_LAST_MOMENT_UTM_SOURCE VARCHAR ,SHOPIFYQL_LAST_VISIT_NON_UTM_SOURCE VARCHAR ,SHOPIFYQL_FIRSTVISIT_UTM_MEDIUM VARCHAR ,SHOPIFYQL_LAST_MOMENT_UTM_MEDIUM VARCHAR ,FINAL_UTM_CHANNEL VARCHAR ,FINAL_UTM_CAMPAIGN VARCHAR ,FINAL_UTM_SOURCE VARCHAR ,REFERRER_NAME VARCHAR ,GOKWIK_MAPPED_SOURCE VARCHAR ,GOKWIK_MAPPED_CHANNEL VARCHAR ,REFUND_DETAILS ARRAY ,AWB VARCHAR ,COURIER VARCHAR ,SHIPPING_STATUS VARCHAR ,SHIPPING_STATUS_UPDATE_DATE VARCHAR ,TRACKING_URL VARCHAR ,SHIPPING_CREATED_AT VARCHAR ,GATEWAY VARCHAR ,PAYMENT_MODE VARCHAR ,SKU_CODE VARCHAR ,COMMONSKUID VARCHAR ,PRODUCT_NAME_FINAL VARCHAR ,PRODUCT_CATEGORY VARCHAR ,PRODUCT_SUB_CATEGORY VARCHAR ,SHOPIFY_NEW_CUSTOMER_FLAG VARCHAR ,SHOPIFY_NEW_CUSTOMER_FLAG_MONTH VARCHAR ,SHOPIFY_ACQUISITION_PRODUCT VARCHAR ,SHOPIFY_ACQUISITION_CHANNEL VARCHAR ,SHOPIFY_ACQUISITION_SOURCE VARCHAR ,SHIPPING_TAX FLOAT ,SHIP_PROMOTION_DISCOUNT FLOAT ,GIFT_WRAP_PRICE FLOAT ,GIFT_WRAP_TAX FLOAT) ; CREATE or replace TABLE charak_db.MAPLEMONK.shipment_status_mapping (SHIPPING_STATUS VARCHAR(16777216),Mapped_Status VARCHAR(16777216)); Insert into charak_db.MAPLEMONK.shipment_status_mapping values (\'DELIVERED\',\'Delivered\') ,(\'CANCELLED\',\'Cancelled\') ,(\'DELIVERED TO ORIGIN\',\'Returned\') ,(\'SHIPMENT CREATED\',\'Shipment Created\') ,(\'RETURNED\',\'Returned\') ,(\'IN TRANSIT\',\'In Transit\') ,(\'DELIVERY DELAYED\',\'In Transit\') ,(\'DELIVERED TO ORIGIN\',\'RTO\') ,(\'CONFIRMED\',\'Open\') ,(\'IN_TRANSIT\',\'In Transit\') ,(\'RTO DELIVERED\',\'RTO\') ,(\'CANCELED\',\'Cancelled\') ,(\'RTO IN TRANSIT\',\'RTO\') ,(\'REACHED AT DESTINATION HUB\',\'In Transit\') ,(\'PICKED UP\',\'In Transit\') ,(\'UNDELIVERED-2ND ATTEMPT\',\'In Transit\') ,(\'RTO_OFD\',\'RTO\') ,(\'OUT FOR DELIVERY\',\'Out for delivery\') ,(\'RTO INITIATED\',\'RTO\') ,(\'PICKUP EXCEPTION\',\'Pickup Error\') ,(\'CANCELLATION REQUESTED\',\'Pickup Error\') ,(\'READY TO SHIP\',\'Ready to Ship\') ,(\'OUT_FOR_DELIVERY\',\'Out for delivery\') ,(\'SHIPPED\',\'In Transit\') ,(\'PICKUP RESCHEDULED\',\'Pickup Error\') ,(\'UNDELIVERED-3RD ATTEMPT\',\'In Transit\') ,(\'MISROUTED\',\'Misrouted\') ,(\'PICKUP SCHEDULED\',\'Pickup Error\') ,(\'UNDELIVERED-1ST ATTEMPT\',\'In Transit\') ,(\'Not Traceble\',\'Pickup Error\') ,(\'UNDELIVERED\',\'In Transit\') ,(\'LOST\',\'Lost\') ,(\'IN TRANSIT-EN-ROUTE\',\'In Transit\') ,(\'Indian Speed Post\',\'Indian Speed Post\') ,(\'Exception\',\'Exception\') ,(\'Shipment Booked\',\'Pickup Error\') ,(\'Assigned\',\'Assigned\') ,(\'Pending\',\'Pending\') ,(\'Printed\',\'Printed\') ,(\'IN TRANSIT-AT DESTINATION HUB\',\'In Transit\') ,(\'Upcoming\',\'Order Yet To sync\') ,(\'RTO-In Transit\',\'RTO\') ,(\'PICKUP ERROR\',\'Pickup Error\') ,(\'RTO-Out for Delivery\',\'RTO\') ,(\'RTO-Delivered\',\'RTO\') ,(\'OUT FOR PICKUP\',\'Pickup Error\') ,(\'RTO_NDR\',\'RTO\') ,(\'RTO-Exception\',\'Exception\') ,(\'Delayed\',\'In Transit\') ,(\'IN TRANSIT-AT SOURCE HUB\',\'In Transit\') ,(\'RTS\',\'Underprocess\') ,(\'Manifested\',\'Pickup Error\') ,(\'Dispatched\',\'In Transit\') ,(\'RTO\',\'RTO\') ,(\'Not Picked\',\'Pickup Error\') ,(\'DAMAGED\',\'Damaged\') ,(\'Closed\',\'Cancelled\') ,(\'Open\',\'Open\') ,(\'On Hold\',\'On Hold\') ,(\'Shipped - Returned to Seller\',\'RTO\') ,(\'Shipped - Delivered to Buyer\',\'Delivered\') ,(\'Shipped - Picked Up\',\'In Transit\') ,(\'Shipped - Rejected by Buyer\',\'RTO\') ,(\'Shopify_Processed\',\'Shopify_Processed\') ,(\'Ready to dispatch\',\'Ready to Dispatch\') ,(\'RTO Undelivered\',\'RTO\') ,(\'Shipment Lost\',\'Lost\') ,(\'Shipment Error\',\'Shipment Error\') ,(\'RTO In-Transit\',\'RTO\') ,(\'Dispatch / RTO\',\'RTO\') ,(\'Pending Pick-up\',\'Pending\') ,(\'Dispatch/Intransit\',\'In Transit\') ,(\'Dispatch /Lost and Damange\',\'Lost\') ,(\'Dispatch /Undelivered\',\'Undelivered\') ,(\'Non serviceable\',\'Non Serviceable\') ,(\'Not serviceable\',\'Non Serviceable\') ,(\'Dispatch/Delivered\',\'Delivered\') ,(\'Refund\',\'Refund\') ,(\'F&P\',\'F&P\') ,(\'SHIPPED - RETURNING TO SELLER\',\'RTO\') ,(\'SHIPPED - LOST IN TRANSIT\',\'Lost\') ,(\'SHIPPED - DAMAGED\',\'Damaged\') ,(\'SHIPPED - OUT FOR DELIVERY\',\'Out for delivery\'); create or replace table charak_db.MAPLEMONK.charak_db_sales_consolidated_intermediate as select b.customer_id ,SHOP_NAME ,marketplace ,CHANNEL ,SOURCE ,b.ORDER_ID::varchar ordeR_id ,b.reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,b.SHIPPING_LAST_UPDATE_DATE ,b.SKU ,PRODUCT_ID ,b.PRODUCT_NAME ,CURRENCY ,b.CITY ,b.State ,b.ORDER_STATUS ,Order_Date ,b.QUANTITY ,GROSS_SALES_BEFORE_TAX ,DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,SELLING_PRICE ,UPPER(d.order_status) as OMS_order_status ,UPPER(coalesce(d.shipping_status, b.order_Status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,d.shipping_status,b.order_status)) FINAL_SHIPPING_STATUS ,b.SALEORDERITEMCODE ,b.SALES_ORDER_ITEM_ID ,d.awb AWB ,PAYMENT_GATEWAY ,b.Payment_Mode ,upper(d.courier) AS COURIER ,d.dispatch_date AS DISPATCH_DATE ,coalesce(d.delivered_date,case when b.order_status like \'delivered\' then b.SHIPPING_LAST_UPDATE_DATE end) AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,b.order_status, d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end, d.return_flag) AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(selling_price,0) end returned_sales ,case when lower(b.order_status) in (\'canceled\') then quantity::int end cancelled_quantity ,NEW_CUSTOMER_FLAG ,null acquisition_product ,null Days_in_Shipment ,null ACQUSITION_DATE ,d.sku SKU_CODE ,UPPER(d.product_name_final) PRODUCT_NAME_FINAL ,upper(d.product_category) PRODUCT_CATEGORY ,upper(d.product_sub_category) product_sub_catgory from charak_db.maplemonk.magento_Fact_items b left join (select * from (select order_id ,sku ,city ,state ,reference_code ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,PAYMENT_MODE ,email ,product_name_final ,product_category ,product_sub_category ,row_number() over (partition by order_id, sku order by shipping_last_update_date desc) rw from charak_db.MAPLEMONK.charak_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%magento%\',\'%moha%\',\'%vedistry%\')) where rw=1 ) d on b.reference_code=d.reference_code and upper(b.sku)=upper(d.sku) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from charak_db.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(d.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) union all select b.customer_id ,upper(SHOP_NAME) shop_name ,upper(shop_name) marketplace ,null CHANNEL ,null SOURCE ,b.ORDER_ID::varchar ordeR_id ,b.order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,null SHIPPING_LAST_UPDATE_DATE ,b.SKU ,PRODUCT_ID ,b.PRODUCT_NAME ,CURRENCY ,b.CITY ,b.State ,b.ORDER_STATUS ,order_timestamp::date Order_Date ,b.QUANTITY ,GROSS_SALES_BEFORE_TAX ,DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.total_sales SELLING_PRICE ,UPPER(d.order_status) as OMS_order_status ,UPPER(coalesce(d.shipping_status, b.order_Status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,d.shipping_status,b.order_status)) FINAL_SHIPPING_STATUS ,b.line_item_id SALEORDERITEMCODE ,b.line_item_id SALES_ORDER_ITEM_ID ,d.awb AWB ,null PAYMENT_GATEWAY ,b.PAYMENT_METHOD ,upper(d.courier) AS COURIER ,d.dispatch_date AS DISPATCH_DATE ,coalesce(d.delivered_date,case when b.order_status like \'delivered\' then null end) AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,b.order_status, d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,d.return_flag AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(selling_price,0) end returned_sales ,case when lower(b.order_status) in (\'canceled\') then quantity::int end cancelled_quantity ,null NEW_CUSTOMER_FLAG ,null acquisition_product ,null Days_in_Shipment ,null ACQUSITION_DATE ,d.sku SKU_CODE ,UPPER(d.product_name_final) PRODUCT_NAME_FINAL ,upper(d.product_category) PRODUCT_CATEGORY ,upper(d.product_sub_category) product_sub_category from charak_db.maplemonk.FACT_ITEMS_woocommerce_charak b left join (select * from (select order_id ,sku ,city ,state ,reference_code ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,PAYMENT_MODE ,email ,product_name_final ,product_category ,product_sub_category ,row_number() over (partition by order_id, sku order by shipping_last_update_date desc) rw from charak_db.MAPLEMONK.charak_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%charak%\')) where rw=1 ) d on b.order_id=d.order_id and upper(b.sku) = upper(d.sku) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from charak_db.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(d.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) union all select Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,\'AMAZON\' AS SOURCE ,afi.ORDER_ID ,afi.ORDER_ID reference_code ,Null as PHONE ,NAME ,coalesce(EEFI.EMAIL,UFI.EMAIL,AFI.EMAIL) AS EMAIL ,coalesce(EEFI.shipping_last_update_date::datetime, UFI.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(afi.STATE) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP::date AS Order_Date ,afi.QUANTITY ,ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,afi.TAX ,afi.SHIPPING_PRICE ,TOTAL_SALES AS SELLING_PRICE ,upper(coalesce(EEFI.order_status,UFI.order_status)) as OMS_order_status ,upper(coalesce(EEFI.shipping_status,UFI.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) FINAL_SHIPPING_STATUS ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALEORDERITEMCODE ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALES_ORDER_ITEM_ID ,coalesce(EEFI.awb,UFI.awb) AWB ,NULL Payment_Gateway ,upper(coalesce(EEFI.payment_mode,UFI.payment_mode)) Payment_Mode ,Upper(coalesce(EEFI.Courier,UFI.courier)) AS COURIER ,coalesce(EEFI.manifest_date,UFI.dispatch_date) AS DISPATCH_DATE ,coalesce(EEFI.delivered_date,UFI.delivered_date) AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,ufi.shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,afi.IS_REFUND AS RETURN_FLAG ,case when afi.is_refund = 1 then quantity::int end returned_quantity ,case when afi.is_refund = 1 then total_sales end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce(shipmap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(afi.ORDER_TIMESTAMP),date(coalesce(ufi.shipping_last_update_date::datetime, eefi.shipping_last_update_date::datetime))) when lower(coalesce( shipmap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(afi.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,coalesce(afi.SKU,ufi.PRODUCT_ID,eefi.SKU) as SKU_CODE ,UPPER(AFI.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI.PRODUCT_SUB_CATEGORY) product_sub_Category from charak_db.MAPLEMONK.charak_db_AMAZON_FACT_ITEMS AFI left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from charak_db.MAPLEMONK.charak_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\') ) EEFI on AFI.Order_id = EEFI.reference_code and AFI.PRODUCT_ID = EEFI.sku left join (select * from (select order_id ,city ,state ,reference_code ,product_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,payment_mode ,email ,product_name_final ,product_category ,product_sub_category ,row_number() over (partition by order_id, product_id order by shipping_last_update_date desc) rw from charak_db.MAPLEMONK.charak_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%amazon%\')) where rw=1 ) UFI on AFI.order_id = UFI.order_id and AFI.SKU = UFI.PRODUCT_ID left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from charak_db.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(EEFI.shipping_status,UFI.shipping_status,afi.ORDER_STATUS)) = lower(ShipMap.shipping_status) union all select Null as customer_id ,upper(marketplace) shop_name ,upper(marketplace) marektplace ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,phone as PHONE ,name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.PRODUCT_ID ,PRODUCT_NAME AS PRODUCT_NAME ,CURRENCY ,upper(CITY) as city ,upper(STATE) AS State ,upper(ORDER_STATUS) order_status ,ORDER_DATE::date AS Order_Date ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0) - ifnull(tax,0) gross_sales_before_tax ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_ORDER_STATUS ,upper(b.shipping_status) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status)) FINAL_SHIPPING_STATUS ,saleOrderItemCode as SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID as SALES_ORDER_ITEM_ID ,AWB ,null as payment_gateway ,payment_mode ,COURIER ,DISPATCH_DATE AS DISPATCH_DATE ,delivered_date as delivered_date ,case when upper(FINAL_SHIPPING_STATUS) in (\'DELIVERED\') then 1 end AS DELIVERED_STATUS ,return_flag AS RETURN_FLAG ,case when return_flag = 1 then suborder_quantity::int end returned_quantity ,case when return_flag = 1 then selling_price::float end returned_sales ,case when return_flag = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when order_status=\'COMPLETE\' then delivered_date::date-order_date::date else current_date - order_date::Date end as days_in_shipment ,NULL AS ACQUSITION_DATE ,sku_code ,upper(b.product_name_final) PRODUCT_NAME_FINAL ,upper(b.Product_Category) PRODUCT_CATEGORY ,upper(b.product_sub_category) product_sub_category from charak_db.MAPLEMONK.charak_db_unicommerce_fact_items b left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from charak_db.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) where not(lower(b.marketplace) like any (\'%magento%\',\'%vedistry%\',\'%charak%\',\'%moha%\')) ; create or replace table charak_db.MAPLEMONK.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from charak_db.MAPLEMONK.charak_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from charak_db.MAPLEMONK.charak_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table charak_db.MAPLEMONK.charak_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from charak_db.MAPLEMONK.charak_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from charak_db.MAPLEMONK.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from charak_db.MAPLEMONK.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE charak_db.MAPLEMONK.charak_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM charak_db.MAPLEMONK.charak_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM charak_db.MAPLEMONK.charak_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE charak_db.MAPLEMONK.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM charak_db.MAPLEMONK.charak_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM charak_db.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM charak_db.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE charak_db.MAPLEMONK.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM charak_db.MAPLEMONK.charak_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE charak_db.MAPLEMONK.charak_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM charak_db.MAPLEMONK.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; CREATE table if not exists charak_db.MAPLEMONK.charak_db_easyecom_returns_intermediate (ORDER_ID NUMBER,INVOICE_ID NUMBER,SUBORDER_ID VARIANT,REFERENCE_CODE VARCHAR,CREDIT_NOTE_ID NUMBER,ORDER_DATE TIMESTAMP_NTZ,INVOICE_DATE TIMESTAMP_NTZ,RETURN_DATE TIMESTAMP_NTZ,MANIFEST_DATE TIMESTAMP_NTZ,IMPORT_DATE TIMESTAMP_NTZ,LAST_UPDATE_DATE TIMESTAMP_NTZ,COMPANY_PRODUCT_ID VARIANT,PRODUCTNAME VARCHAR,PRODUCT_ID VARIANT,SKU VARCHAR,MARKETPLACE VARCHAR,MARKETPLACE_ID NUMBER,REPLACEMENT_ORDER NUMBER,RETURN_REASON VARCHAR,RETURNED_QUANTITY FLOAT,RETURN_AMOUNT_WITHOUT_TAX FLOAT,RETURN_TAX FLOAT,RETURN_SHIPPING_CHARGE FLOAT,RETURN_MISC FLOAT,TOTAL_RETURN_AMOUNT FLOAT) ; Create or replace table charak_db.MAPLEMONK.charak_db_easyecom_returns_fact_items as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from charak_db.MAPLEMONK.charak_db_easyecom_returns_intermediate FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from charak_db.MAPLEMONK.charak_db_sales_consolidated) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table charak_db.MAPLEMONK.charak_db_RETURNS_CONSOLIDATED as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from charak_db.MAPLEMONK.charak_db_easyecom_returns_fact_items group by 1,2,3 order by 2 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from charak_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        