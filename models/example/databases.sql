{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE table if not exists dunatura_db.maplemonk.dunatura_db_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, product_category varchar, product_sub_category varchar) ; create table if not exists dunatura_db.maplemonk.dunatura_db_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar, awb varchar,Marketplace_LineItem_ID varchar, reference_code varchar,LAST_UPDATE_DATE date,PAYMENT_MODE varchar,COURIER varchar,MANIFEST_DATE date, DELIVERED_DATE date,mapped_product_name varchar,mapped_category varchar, mapped_sub_category varchar) ; create table if not exists dunatura_db.maplemonk.dunatura_db_UNICOMMERCE_FACT_ITEMS ( order_id varchar ,city varchar ,state varchar ,saleorderitemcode varchar ,sales_order_item_id varchar ,shippingpackagecode varchar ,SHIPPINGPACKAGESTATUS varchar ,shipping_status varchar ,order_status varchar ,Courier varchar ,Dispatch_Date date ,Delivered_date date ,Return_flag int ,Return_quantity int ,cancelled_quantity int ,shipping_last_update_date date ,days_in_shipment float ,awb varchar ,marketplace varchar ,payment_method varchar ,PAYMENT_MODE varchar ,PRODUCT_ID varchar ,mapped_product_name varchar ,mapped_category varchar ,email varchar ,mapped_sub_category varchar) ; create or replace table dunatura_db.maplemonk.dunatura_db_sales_consolidated_intermediate as select b.customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.shop_name) as marketplace ,Upper(b.FINAL_UTM_CHANNEL) AS CHANNEL ,Upper(b.FINAL_UTM_SOURCE) AS SOURCE ,subscription_order ,subscription_first_order ,subscription_recurring_order ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,week(b.ORDER_TIMESTAMP::date) as order_week ,CASE DAYOFWEEK(b.ORDER_TIMESTAMP::date) WHEN 0 THEN \'7.Sunday\' WHEN 1 THEN \'1.Monday\' WHEN 2 THEN \'2.Tuesday\' WHEN 3 THEN \'3.Wednesday\' WHEN 4 THEN \'4.Thursday\' WHEN 5 THEN \'5.Friday\' WHEN 6 THEN \'6.Saturday\' END AS day_name ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) AS SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,d.sales_order_item_id as SALES_ORDER_ITEM_ID ,coalesce(b.awb,c.awb,d.awb) AWB ,UPPER(b.GATEWAY) PAYMENT_GATEWAY ,upper(coalesce(c.payment_mode,d.payment_mode)) Payment_Mode ,Upper(coalesce(c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(b.Shipping_created_at,c.manifest_date,d.dispatch_date) AS DISPATCH_DATE ,coalesce(c.delivered_date,d.delivered_date,case when b.shipping_status like \'delivered\' then b.shipping_status_update_date end) AS DELIVERED_DATE ,case when lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime))) when lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,b.fulfillment_status from dunatura_db.maplemonk.dunatura_db_SHOPIFY_FACT_ITEMS b left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from dunatura_db.maplemonk.dunatura_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%shopify%\') ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,PAYMENT_MODE ,email ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from dunatura_db.maplemonk.dunatura_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=split_part(d.saleorderitemcode,\'-\',0) union all select Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,\'AMAZON\' AS SOURCE ,null as subscription_order ,null as subscription_first_order ,null as subscription_recurring_order ,afi.ORDER_ID ,afi.ORDER_ID reference_code ,Null as PHONE ,null as NAME ,coalesce(EEFI.EMAIL,UFI.EMAIL) AS EMAIL ,coalesce(EEFI.shipping_last_update_date::datetime, UFI.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(afi.STATE) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP::date AS Order_Date ,week(afi.ORDER_TIMESTAMP::date) as order_Week ,CASE DAYOFWEEK(afi.ORDER_TIMESTAMP::date) WHEN 0 THEN \'7.Sunday\' WHEN 1 THEN \'1.Monday\' WHEN 2 THEN \'2.Tuesday\' WHEN 3 THEN \'3.Wednesday\' WHEN 4 THEN \'4.Thursday\' WHEN 5 THEN \'5.Friday\' WHEN 6 THEN \'6.Saturday\' END AS day_name ,afi.QUANTITY ,ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,afi.TAX ,afi.SHIPPING_PRICE ,TOTAL_SALES AS SELLING_PRICE ,upper(coalesce(EEFI.order_status,UFI.order_status)) as OMS_order_status ,upper(coalesce(EEFI.shipping_status,UFI.shipping_status)) AS SHIPPING_STATUS ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALEORDERITEMCODE ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALES_ORDER_ITEM_ID ,coalesce(EEFI.awb,UFI.awb) AWB ,NULL Payment_Gateway ,upper(coalesce(EEFI.payment_mode,UFI.payment_mode)) Payment_Mode ,Upper(coalesce(EEFI.Courier,UFI.courier)) AS COURIER ,coalesce(EEFI.manifest_date,UFI.dispatch_date) AS DISPATCH_DATE ,coalesce(EEFI.delivered_date,UFI.delivered_date) AS DELIVERED_DATE ,case when lower(coalesce(ufi.shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,afi.IS_REFUND AS RETURN_FLAG ,case when afi.is_refund = 1 then quantity::int end returned_quantity ,case when afi.is_refund = 1 then total_sales end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce( EEFI.shipping_status,UFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(afi.ORDER_TIMESTAMP),date(coalesce(ufi.shipping_last_update_date::datetime, eefi.shipping_last_update_date::datetime))) when lower(coalesce( EEFI.shipping_status,UFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(afi.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,coalesce(afi.SKU,ufi.PRODUCT_ID,eefi.SKU) as SKU_CODE ,UPPER(AFI.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,null as fulfillment_status from dunatura_db.maplemonk.dunatura_db_AMAZON_FACT_ITEMS AFI left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from dunatura_db.maplemonk.dunatura_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\') ) EEFI on AFI.Order_id = EEFI.reference_code and AFI.PRODUCT_ID = EEFI.sku left join (select * from (select order_id ,city ,state ,product_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,payment_mode ,email ,row_number() over (partition by order_id, product_id order by shipping_last_update_date desc) rw from dunatura_db.maplemonk.dunatura_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%amazon%\')) where rw=1 ) UFI on AFI.order_id = UFI.order_id and AFI.SKU = UFI.PRODUCT_ID ; create or replace table dunatura_db.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from dunatura_db.maplemonk.dunatura_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from dunatura_db.maplemonk.dunatura_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table dunatura_db.maplemonk.dunatura_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* ,e.quiz_started ,e.quiz_finished ,e.tagespack_produced ,f.direct_hours ,f.indirect_hours ,f.shopify_backlog ,div0(f.tagespack_produced_B2B, count(case when selling_price <> 0 then 1 end) over (partition by m.order_date::date order by 1)) tagespack_produced_B2B ,div0(f.tagespack_produced_B2C, count(case when selling_price <> 0 then 1 end) over (partition by m.order_date::date order by 1)) tagespack_produced_B2C ,div0(f.returns,count(1) over (partition by m.order_date::date order by 1)) returns ,f.oldest_open_order ,f.oldest_onhold_order from ( select c.maple_monk_id as maple_monk_id_phone, o.* from dunatura_db.maplemonk.dunatura_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from dunatura_db.maplemonk.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from dunatura_db.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email left join (select * from (select date, quiz_started, quiz_finished, tagespack_produced, row_number() over (partition by date order by _AIRBYTE_EMITTED_AT desc) rw from dunatura_db.maplemonk.api_data)where rw=1) e on m.ordeR_date::date = e.date left join (select to_date(date,\'dd.mm.yyyy\') date ,\"direct hours\" direct_hours ,\"indirect hours\" indirect_hours ,\"backlog Shopify\" shopify_backlog ,\"produced TPs B2B\" tagespack_produced_B2B ,\"produced TPs B2C\" tagespack_produced_B2C ,\"amount of retours\" returns ,to_date(\"oldest order (open)\",\'dd.mm.yyyy\') oldest_open_order ,to_date(\"oldest order (on hold)\",\'dd.mm.yyyy\') oldest_onhold_order from dunatura_db.maplemonk.metrics_sheet_data_input ) f on m.ordeR_date::date = f.date ; ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE dunatura_db.maplemonk.dunatura_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM dunatura_db.maplemonk.dunatura_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM dunatura_db.maplemonk.dunatura_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE dunatura_db.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM dunatura_db.maplemonk.dunatura_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM dunatura_db.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM dunatura_db.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE dunatura_db.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM dunatura_db.maplemonk.dunatura_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE dunatura_db.maplemonk.dunatura_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM dunatura_db.maplemonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; CREATE table if not exists dunatura_db.maplemonk.dunatura_db_easyecom_returns_fact_items (MARKETING_CHANNEL VARCHAR,ORDER_ID NUMBER,INVOICE_ID NUMBER,SUBORDER_ID VARIANT,REFERENCE_CODE VARCHAR,CREDIT_NOTE_ID NUMBER,ORDER_DATE TIMESTAMP_NTZ,INVOICE_DATE TIMESTAMP_NTZ,RETURN_DATE TIMESTAMP_NTZ,MANIFEST_DATE TIMESTAMP_NTZ,IMPORT_DATE TIMESTAMP_NTZ,LAST_UPDATE_DATE TIMESTAMP_NTZ,COMPANY_PRODUCT_ID VARIANT,PRODUCTNAME VARCHAR,PRODUCT_ID VARIANT,SKU VARCHAR,MARKETPLACE VARCHAR,MARKETPLACE_ID NUMBER,REPLACEMENT_ORDER NUMBER,RETURN_REASON VARCHAR,RETURNED_QUANTITY FLOAT,RETURN_AMOUNT_WITHOUT_TAX FLOAT,RETURN_TAX FLOAT,RETURN_SHIPPING_CHARGE FLOAT,RETURN_MISC FLOAT,TOTAL_RETURN_AMOUNT FLOAT) ; create or replace table dunatura_db.maplemonk.dunatura_db_RETURNS_CONSOLIDATED as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from dunatura_db.maplemonk.dunatura_db_easyecom_returns_fact_items group by 1,2,3 order by 2 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from dunatura_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        