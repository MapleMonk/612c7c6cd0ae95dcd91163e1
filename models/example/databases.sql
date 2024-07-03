{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE table if not exists skinq_db.Maplemonk.skinq_db_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, product_category varchar, product_sub_category varchar) ; create table if not exists skinq_db.Maplemonk.skinq_db_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar, awb varchar,Marketplace_LineItem_ID varchar, reference_code varchar,LAST_UPDATE_DATE date,PAYMENT_MODE varchar,COURIER varchar,MANIFEST_DATE date, DELIVERED_DATE date,mapped_product_name varchar,mapped_category varchar, mapped_sub_category varchar, warehouse_name varchar) ; create table if not exists skinq_db.Maplemonk.skinq_db_UNICOMMERCE_FACT_ITEMS ( order_id varchar ,city varchar ,state varchar ,saleorderitemcode varchar ,sales_order_item_id varchar ,shippingpackagecode varchar ,SHIPPINGPACKAGESTATUS varchar ,shipping_status varchar ,order_status varchar ,Courier varchar ,Dispatch_Date date ,Delivered_date date ,Return_flag int ,Return_quantity int ,cancelled_quantity int ,shipping_last_update_date date ,days_in_shipment float ,awb varchar ,marketplace varchar ,payment_method varchar ,PAYMENT_MODE varchar ,PRODUCT_ID varchar ,mapped_product_name varchar ,mapped_category varchar ,email varchar ,mapped_sub_category varchar ,warehouse varchar) ; create or replace table skinq_db.Maplemonk.skinq_db_sales_consolidated_pre_intermediate as select b.customer_id::varchar customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.shop_name) as marketplace ,Upper(b.FINAL_UTM_CHANNEL) AS CHANNEL ,Upper(b.FINAL_UTM_SOURCE) AS SOURCE ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(SR.status,b.shipping_status, c.shipping_status,d.shipping_status)) AS SHIPPING_STATUS ,UPPER(coalesce(Shipmap.final_shipping_status,SR.status,b.shipping_status, c.shipping_status,d.shipping_status)) FINAL_SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,d.sales_order_item_id::varchar as SALES_ORDER_ITEM_ID ,coalesce(b.awb,c.awb,d.awb) AWB ,UPPER(b.GATEWAY) PAYMENT_GATEWAY ,upper(coalesce(b.payment_mode, c.payment_mode)) Payment_Mode ,Upper(coalesce(sr.courier, c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(sr.dispatch_date, d.dispatch_date,b.Shipping_created_at,c.manifest_date) AS DISPATCH_DATE ,coalesce(Sr.Shipment_updated_date, c.delivered_date,d.delivered_date,case when lower(FINAL_SHIPPING_STATUS) like \'delivered\' then coalesce(sr.Shipment_updated_date,b.shipping_status_update_date) end) AS DELIVERED_DATE ,case when lower(FINAL_SHIPPING_STATUS) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') and lower(final_shipping_status) <> \'cancelled\' then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(selling_price,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(FINAL_SHIPPING_STATUS) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(coalesce(sr.delivered_date,b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime))) when lower(FINAL_SHIPPING_STATUS) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,upper(d.warehouse_name) warehouse from skinq_db.Maplemonk.skinq_db_SHOPIFY_FACT_ITEMS b left join (select * from (select awb ,status ,updated_date Shipment_updated_date ,pickedup_date dispatch_date ,first_out_for_delivery_date ,delivered_date ,courier ,row_number() over (partition by awb order by 1) rw from skinq_db.Maplemonk.skinq_logistics_fact_items ) where rw = 1 ) SR on b.awb =SR.awb left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from skinq_db.Maplemonk.skinq_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%shopify%\') ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_mode payment_method ,email ,warehouse_name ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from skinq_db.Maplemonk.skinq_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=split_part(d.saleorderitemcode,\'-\',0) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from skinq_db.maplemonk.mapping_shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(SR.status,b.shipping_status, c.shipping_status,d.shipping_status)) = lower(ShipMap.shipping_status) union all select Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,\'AMAZON\' AS SOURCE ,afi.ORDER_ID ,afi.ORDER_ID reference_code ,Null as PHONE ,null as NAME ,coalesce(EEFI.EMAIL,UFI.EMAIL) AS EMAIL ,coalesce(sr.Shipment_updated_date,EEFI.shipping_last_update_date::datetime, UFI.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(coalesce(afi.state_mapped, afi.STATE)) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP::date AS Order_Date ,afi.QUANTITY ,ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,afi.TAX ,afi.SHIPPING_PRICE ,TOTAL_SALES AS SELLING_PRICE ,upper(coalesce(EEFI.order_status,UFI.order_status)) as OMS_order_status ,upper(coalesce(sr.status,EEFI.shipping_status,UFI.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(ShipMap.final_shipping_status,EEFI.shipping_status,UFI.shipping_status)) FINAL_SHIPPING_STATUS ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALEORDERITEMCODE ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALES_ORDER_ITEM_ID ,coalesce(EEFI.awb,UFI.awb) AWB ,NULL Payment_Gateway ,upper(coalesce(EEFI.payment_mode,UFI.payment_mode)) Payment_Mode ,Upper(coalesce(SR.COURIER,EEFI.Courier,UFI.courier)) AS COURIER ,coalesce(SR.DISPATCH_DATE, EEFI.manifest_date,UFI.dispatch_date) AS DISPATCH_DATE ,coalesce(SR.DELIVERED_DATE,EEFI.delivered_date,UFI.delivered_date) AS DELIVERED_DATE ,case when lower(coalesce(sr.status,ufi.shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,case when lower(coalesce(afi.ORDER_STATUS,UFI.order_status)) in (\'cancelled\') then 0 else afi.IS_REFUND end AS RETURN_FLAG ,case when afi.is_refund = 1 then quantity::int end returned_quantity ,case when afi.is_refund = 1 then total_sales end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce(sr.status, EEFI.shipping_status,UFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(afi.ORDER_TIMESTAMP),date(coalesce(sr.delivered_date, ufi.shipping_last_update_date::datetime, eefi.shipping_last_update_date::datetime))) when lower(coalesce( sr.status,EEFI.shipping_status,UFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(afi.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,coalesce(afi.sku_code,ufi.PRODUCT_ID,eefi.SKU) as SKU_CODE ,UPPER(AFI.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,upper(UFI.warehouse_name) warehouse from skinq_db.Maplemonk.skinq_db_AMAZON_FACT_ITEMS AFI left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from skinq_db.Maplemonk.skinq_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\') ) EEFI on AFI.Order_id = EEFI.reference_code and AFI.PRODUCT_ID = EEFI.sku left join (select * from (select order_id ,city ,upper(coalesce(state_mapped, state)) state ,product_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,null as payment_method ,payment_mode ,email ,warehouse_name ,row_number() over (partition by order_id, product_id order by shipping_last_update_date desc) rw from skinq_db.Maplemonk.skinq_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%amazon%\')) where rw=1 ) UFI on AFI.order_id = UFI.order_id and AFI.PRODUCT_ID = UFI.PRODUCT_ID left join (select * from (select awb ,status ,updated_date Shipment_updated_date ,pickedup_date dispatch_date ,first_out_for_delivery_date ,delivered_date ,courier ,row_number() over (partition by awb order by 1) rw from skinq_db.Maplemonk.skinq_logistics_fact_items ) where rw = 1 ) SR on UFI.awb =SR.awb left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from skinq_db.maplemonk.mapping_shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(sr.status,EEFI.shipping_status,UFI.shipping_status,AFI.order_status)) = lower(ShipMap.shipping_status) union all select Null as customer_id ,upper(marketplace) shop_name ,upper(marketplace) marektplace ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,phone as PHONE ,name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.PRODUCT_ID ,PRODUCT_NAME AS PRODUCT_NAME ,CURRENCY ,upper(CITY) as city ,upper(coalesce(STATE_MAPPED, state)) AS State ,upper(ORDER_STATUS) order_status ,ORDER_DATE::date AS Order_Date ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0) - ifnull(tax,0) gross_sales_before_tax ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_ORDER_STATUS ,upper(coalesce(sr.status, b.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status)) FINAL_SHIPPING_STATUS ,saleOrderItemCode as SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID as SALES_ORDER_ITEM_ID ,b.AWB ,null as payment_gateway ,payment_mode ,coalesce(sr.COURIER, b.courier) courier ,coalesce(sr.DISPATCH_DATE, b.dispatch_date) AS DISPATCH_DATE ,coalesce(sr.delivered_date, b.delivered_date) as delivered_date ,case when upper(FINAL_SHIPPING_STATUS) in (\'DELIVERED\') then 1 end AS DELIVERED_STATUS ,case when lower(coalesce(order_status,b.shipping_status)) in (\'cancelled\') then 0 else return_flag end AS RETURN_FLAG ,case when return_flag = 1 then suborder_quantity::int end returned_quantity ,case when return_flag = 1 then selling_price::float end returned_sales ,case when return_flag = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce(sr.status,order_status)) like any (\'complete\',\'delivered\') then coalesce(sr.delivered_date::date,b.delivered_date)-order_date::date else current_date - order_date::Date end as days_in_shipment ,NULL AS ACQUSITION_DATE ,sku_code ,upper(b.product_name_final) PRODUCT_NAME_FINAL ,upper(b.Product_Category) PRODUCT_CATEGORY ,upper(b.product_sub_category) PRODUCT_SUB_CATEGORY ,upper(warehouse_name) warehouse from skinq_db.MapleMonk.skinq_db_unicommerce_fact_items b left join (select * from (select awb ,status ,updated_date Shipment_updated_date ,pickedup_date dispatch_date ,first_out_for_delivery_date ,delivered_date ,courier ,row_number() over (partition by awb order by 1) rw from skinq_db.Maplemonk.skinq_logistics_fact_items ) where rw = 1 ) SR on b.awb =SR.awb left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from skinq_db.maplemonk.mapping_shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(sr.status,b.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) where lower(marketplace) not like (\'%shopify%\') and lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%amz%\') ; create or replace table skinq_db.Maplemonk.skinq_db_sales_consolidated_intermediate as select CUSTOMER_ID ,SHOP_NAME ,MARKETPLACE ,CHANNEL ,SOURCE ,ORDER_ID ,reference_code ,PHONE ,NAME ,EMAIL ,SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE ,ORDER_STATUS ,ORDER_DATE ,OMS_ORDER_STATUS ,SHIPPING_STATUS ,FINAL_SHIPPING_STATUS ,SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID ,AWB ,warehouse ,PAYMENT_GATEWAY ,PAYMENT_MODE ,COURIER ,DISPATCH_DATE ,DELIVERED_DATE ,DELIVERED_STATUS ,RETURN_FLAG ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,DAYS_IN_SHIPMENT ,ACQUSITION_DATE ,sc.SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,PRODUCT_SUB_CATEGORY ,coalesce(pcm.skucode_child, sku) SKU_CODE_CHILD ,coalesce(upper(pcm.productname),upper(PRODUCT_NAME_FINAL)) CHILD_PRODUCT_NAME ,coalesce(upper(pcm.category),upper(PRODUCT_CATEGORY)) CHILD_PRODUCT_CATEGORY ,coalesce(upper(pcm.sub_category), upper(PRODUCT_SUB_CATEGORY)) CHILD_PRODUCT_SUBCATEGORY ,ifnull(pcm.qty,1)*ifnull(sc.quantity,0) as QUANTITY_CHILD ,ifnull(pcm.qty,1)*ifnull(sc.RETURNED_QUANTITY,0) as RETURNED_QUANTITY_CHILD ,ifnull(pcm.qty,1)*ifnull(sc.CANCELLED_QUANTITY,0) as CANCELLED_QUANTITY_CHILD ,div0(ifnull(QUANTITY,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) Quantity ,div0(ifnull(GROSS_SALES_BEFORE_TAX,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) GROSS_SALES_BEFORE_TAX ,div0(ifnull(DISCOUNT,0), count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) DISCOUNT ,div0(ifnull(TAX,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) TAX ,div0(ifnull(SHIPPING_PRICE,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) SHIPPING_PRICE ,div0(ifnull(SELLING_PRICE,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) SELLING_PRICE ,div0(ifnull(RETURNED_QUANTITY,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) RETURNED_QUANTITY ,div0(ifnull(RETURNED_SALES,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) RETURNED_SALES ,div0(ifnull(CANCELLED_QUANTITY,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) CANCELLED_QUANTITY ,div0(ifnull(MRP.mrp,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID))*ifnull(sc.quantity,0) MRP_sales ,div0(ifnull(MRP.mrp,0),count(1) over (partition by sc.ORDER_ID,sc.SALEORDERITEMCODE,sc.SALES_ORDER_ITEM_ID))*ifnull(sc.quantity,0) - (div0(ifnull(sc.SELLING_PRICE,0),count(1) over (partition by sc.ORDER_ID,sc.SALEORDERITEMCODE,sc.SALES_ORDER_ITEM_ID)) - div0(ifnull(sc.SHIPPING_PRICE,0),count(1) over (partition by sc.ORDER_ID,sc.SALEORDERITEMCODE,sc.SALES_ORDER_ITEM_ID))) mrp_discount ,div0(ifnull(MRP.mrp,0),count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID)) mrp from skinq_db.Maplemonk.skinq_db_sales_consolidated_pre_intermediate sc left join (select SMP.*, SM.productname, SM.Category, SM.Sub_category from (select * from (select skucode ,skucode_child ,qty ,row_number() over (partition by skucode, skucode_child order by 1) rw from skinq_db.MAPLEMONK.skinq_db_sku_mapping_parent_child_including_marketplace_sku ) where rw=1 ) SMP left join (select * from (select primarykey skucode, \"PRODUCT TITLE\" productname, category, sub_category, row_number() over (partition by primarykey order by 1) rw from skinq_db.Maplemonk.skinq_db_sku_master) where rw = 1 ) SM on replace(SMP.skucode_child,\' \',\'\') = replace(SM.skucode,\' \',\'\') ) pcm on replace(sc.sku_code,\' \',\'\') = replace(pcm.skucode,\' \',\'\') left join ( select * from (select sku_code , try_to_date(start_date,\'DD-MON-YY\') start_Date , try_to_date(end_date,\'DD-MON-YY\') End_date , try_to_double(mrp) mrp , try_to_double(cogs) cogs , row_number() over (partition by sku_code, start_date, end_date order by mrp desc) rw from skinq_db.maplemonk.MAPPING_SKU_MRP_COGS ) where rw=1 ) mrp on replace(sc.sku_code,\' \',\'\') = replace(mrp.sku_code,\' \',\'\') and to_date(sc.order_date)::date >= mrp.start_date and to_date(sc.order_date)::date <= mrp.end_date ; create or replace table skinq_db.Maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from skinq_db.Maplemonk.skinq_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from skinq_db.Maplemonk.skinq_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table skinq_db.Maplemonk.skinq_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from skinq_db.Maplemonk.skinq_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from skinq_db.Maplemonk.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from skinq_db.Maplemonk.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE skinq_db.Maplemonk.skinq_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM skinq_db.Maplemonk.skinq_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM skinq_db.Maplemonk.skinq_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE skinq_db.Maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM skinq_db.Maplemonk.skinq_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM skinq_db.Maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM skinq_db.Maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE skinq_db.Maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, child_product_name product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM skinq_db.Maplemonk.skinq_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE skinq_db.Maplemonk.skinq_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM skinq_db.Maplemonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; create or replace table skinq_db.MAPLEMONK.skinq_db_unicommerce_returns_detailed as select a.order_date ,a.reference_code ,b.channel marketing_channel ,a.marketplace ,sum(a.return_quantity) RETURN_QUANTITY ,sum(a.return_sales) RETURN_sales ,sum(case when return_flag = 1 then TAX end) TOTAL_RETURN_TAX ,sum(return_sales) - TOTAL_RETURN_TAX as TOTAL_RETURN_AMOUNT_EXCL_TAX from skinq_db.Maplemonk.skinq_db_unicommerce_fact_items a left join (select distinct reference_code, channel from skinq_db.Maplemonk.skinq_db_sales_consolidated) b on lower(replace(a.reference_code,\'#\',\'\')) = lower(replace(b.reference_code,\'#\',\'\')) group by 1,2,3,4; create or replace table skinq_db.MAPLEMONK.skinq_db_RETURNS_CONSOLIDATED as select upper(marketplace) Marketplace ,upper(marketing_channel) marketing_channel ,order_date ,sum(RETURN_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(RETURN_sales) TOTAL_RETURN_AMOUNT ,sum(TOTAL_RETURN_TAX) TOTAL_RETURN_TAX ,TOTAL_RETURN_AMOUNT - sum(TOTAL_RETURN_TAX) as TOTAL_RETURN_AMOUNT_EXCL_TAX from skinq_db.Maplemonk.skinq_db_unicommerce_returns_detailed group by 1,2,3 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from skinq_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        