{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table if not exists GLADFUL_DB.MAPLEMONK.GLADFUL_UNICOMMERCE_FACT_ITEMS ( order_id varchar ,city varchar ,state varchar ,saleorderitemcode varchar ,sales_order_item_id varchar ,shippingpackagecode varchar ,SHIPPINGPACKAGESTATUS varchar ,shipping_status varchar ,order_status varchar ,Courier varchar ,Dispatch_Date date ,Delivered_date date ,Return_flag int ,Return_quantity int ,cancelled_quantity int ,shipping_last_update_date date ,days_in_shipment float ,awb varchar ,marketplace varchar ,payment_method varchar ,PAYMENT_MODE varchar ,PRODUCT_ID varchar ,mapped_product_name varchar ,mapped_category varchar ,mapped_sub_category varchar) ; create or replace table GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated_pre_intermediate as select b.customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.marketplace) as marketplace ,Upper(coalesce(b.FINAL_UTM_CHANNEL, GAC.channel)) AS CHANNEL ,Upper(coalesce(b.FINAL_UTM_SOURCE, GAC.Source)) AS SOURCE ,b.ORDER_ID ,b.order_name ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(b.shipping_status,c.shipping_status,d.shipping_status)) AS SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,d.sales_order_item_id as SALES_ORDER_ITEM_ID ,c.invoice_id ,coalesce(b.awb,c.awb,d.awb) AWB ,upper(coalesce(c.warehouse_name,\'NA\')) as Warehouse_Name ,UPPER(b.GATEWAY) PAYMENT_GATEWAY ,upper(coalesce(c.payment_mode,d.payment_mode)) Payment_Mode ,Upper(coalesce(c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(b.Shipping_created_at,c.manifest_date,d.dispatch_date) AS DISPATCH_DATE ,coalesce(c.delivered_date,d.delivered_date,case when b.shipping_status like \'delivered\' then b.shipping_status_update_date end) AS DELIVERED_DATE ,case when lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime))) when lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY from GLADFUL_DB.MAPLEMONK. GLADFUL_SHOPIFY_FACT_ITEMS b left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from GLADFUL_DB.MAPLEMONK. GLADFUL_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%shopify%\') ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,PAYMENT_MODE ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from GLADFUL_DB.MAPLEMONK. GLADFUL_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=split_part(d.saleorderitemcode,\'-\',0) left join (select * from (select *, Row_number() OVER (partition BY ga_transactionid ORDER BY GA_DATE DESC) rw from GLADFUL_DB.MAPLEMONK.GA_ORDER_BY_SOURCE_CONSOLIDATED_GLADFUL) where rw=1) GAC on lower(replace(b.order_name,\'#\',\'\')) =lower(replace(GAC.GA_TRANSACTIONID,\'#\',\'\')) union all select Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,\'AMAZON\' AS SOURCE ,afi.ORDER_ID ,afi.order_id order_name ,Null as PHONE ,AFI.NAME ,AFI.EMAIL ,coalesce(EEFI.shipping_last_update_date::datetime, UFI.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(afi.STATE) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP::date AS Order_Date ,afi.QUANTITY ,ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,afi.TAX ,afi.SHIPPING_PRICE ,TOTAL_SALES AS SELLING_PRICE ,upper(coalesce(EEFI.order_status,UFI.order_status)) as OMS_order_status ,upper(coalesce(EEFI.shipping_status,UFI.shipping_status)) AS SHIPPING_STATUS ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALEORDERITEMCODE ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALES_ORDER_ITEM_ID ,NULL AS INVOICE_ID ,coalesce(EEFI.awb,UFI.awb) AWB ,upper(coalesce(EEFI.warehouse_name,\'NA\')) warehouse_name ,NULL Payment_Gateway ,upper(coalesce(EEFI.payment_mode,UFI.payment_mode)) Payment_Mode ,Upper(coalesce(EEFI.Courier,UFI.courier)) AS COURIER ,coalesce(EEFI.manifest_date,UFI.dispatch_date) AS DISPATCH_DATE ,coalesce(EEFI.delivered_date,UFI.delivered_date) AS DELIVERED_DATE ,case when lower(coalesce(ufi.shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,afi.IS_REFUND AS RETURN_FLAG ,case when afi.is_refund = 1 then quantity::int end returned_quantity ,case when afi.is_refund = 1 then total_sales end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce( EEFI.shipping_status,UFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(afi.ORDER_TIMESTAMP),date(coalesce(ufi.shipping_last_update_date::datetime, eefi.shipping_last_update_date::datetime))) when lower(coalesce( EEFI.shipping_status,UFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(afi.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,afi.SKU_CODE as SKU_CODE ,UPPER(AFI.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY from GLADFUL_DB.MAPLEMONK. GLADFUL_AMAZON_FACT_ITEMS AFI left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from GLADFUL_DB.MAPLEMONK. GLADFUL_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\') ) EEFI on AFI.order_id = EEFI.reference_code and AFI.PRODUCT_ID = EEFI.sku left join (select * from (select order_id ,city ,state ,product_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,payment_mode ,row_number() over (partition by order_id, product_id order by shipping_last_update_date desc) rw from GLADFUL_DB.MAPLEMONK. GLADFUL_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%amazon%\')) where rw=1 ) UFI on AFI.order_id = UFI.order_id and AFI.SKU = UFI.PRODUCT_ID union all select Null as customer_id ,upper(SHOP_NAME) as SHOP_NAME ,upper(marketplace) AS marketplace ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,upper(PRODUCTNAME) AS PRODUCT_NAME ,CURRENCY ,upper(CITY) City ,upper(STATE) AS State ,upper(ORDER_STATUS) as Order_Status ,ORDER_DATE::date AS Order_Date ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0)-ifnull(tax,0)+ifnull(DISCOUNT,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_Order_Status ,upper(Shipping_status) AS SHIPPING_STATUS ,Marketplace_LineItem_ID as SALEORDERITEMCODE ,suborder_id as SALES_ORDER_ITEM_ID ,invoice_id ,AWB ,upper(warehouse_name) as warehouse_name ,NULL Payment_Gateway ,payment_mode Payment_Mode ,UPPER(COURIER) COURIER ,MANIFEST_DATE as DISPATCH_DATE ,DELIVERED_DATE ,case when lower(shipping_status) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then suborder_quantity::int end returned_quantity ,case when RETURN_FLAG = 1 and lower(order_status) not in (\'cancelled\') then ifnull(is_refund,0) end returned_sales ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(shipping_status) in (\'delivered\',\'delivered to origin\') then datediff(day,date(ORDER_DATE),date(shipping_last_update_date::datetime)) when lower(shipping_status) in (\'in transit\', \'shipment created\') then datediff(day,date(ORDER_DATE), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,SKU_CODE as SKU_CODE ,upper(mapped_product_name) as PRODUCT_NAME_FINAL ,upper(mapped_category) as PRODUCT_CATEGORY ,upper(mapped_sub_category) as PRODUCT_SUB_CATEGORY from GLADFUL_DB.MAPLEMONK. GLADFUL_EasyEcom_FACT_ITEMS b where not(lower(marketplace) like any (\'%shopify%\',\'%amazon%\')); create or replace table GLADFUL_DB.MAPLEMONK.GLADFUL_sales_consolidated_intermediate as select CUSTOMER_ID ,SHOP_NAME ,MARKETPLACE ,CHANNEL ,SOURCE ,ORDER_ID ,ORDER_NAME ,PHONE ,NAME ,EMAIL ,SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE ,ORDER_STATUS ,ORDER_DATE ,OMS_ORDER_STATUS ,SHIPPING_STATUS ,SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID ,INVOICE_ID ,AWB ,WAREHOUSE_NAME ,PAYMENT_GATEWAY ,PAYMENT_MODE ,COURIER ,DISPATCH_DATE ,DELIVERED_DATE ,DELIVERED_STATUS ,RETURN_FLAG ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,DAYS_IN_SHIPMENT ,ACQUSITION_DATE ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,PRODUCT_SUB_CATEGORY ,pcm.skucode_child SKU_CODE_CHILD ,upper(pcm.productname) CHILD_PRODUCT_NAME ,upper(pcm.category) CHILD_PRODUCT_CATEGORY ,upper(pcm.sub_category) CHILD_PRODUCT_SUBCATEGORY ,ifnull(pcm.qty,0)*sc.quantity as QUANTITY_CHILD ,ifnull(pcm.qty,0)*sc.RETURNED_QUANTITY as RETURNED_QUANTITY_CHILD ,ifnull(pcm.qty,0)*sc.CANCELLED_QUANTITY as CANCELLED_QUANTITY_CHILD ,div0(QUANTITY,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) Quantity ,div0(GROSS_SALES_BEFORE_TAX,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) GROSS_SALES_BEFORE_TAX ,div0(DISCOUNT, count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) DISCOUNT ,div0(TAX,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) TAX ,div0(SHIPPING_PRICE,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) SHIPPING_PRICE ,div0(SELLING_PRICE,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) SELLING_PRICE ,div0(RETURNED_QUANTITY,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) RETURNED_QUANTITY ,div0(RETURNED_SALES,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) RETURNED_SALES ,div0(CANCELLED_QUANTITY,count(1) over (partition by ORDER_ID,SALEORDERITEMCODE,SALES_ORDER_ITEM_ID,INVOICE_ID)) CANCELLED_QUANTITY from GLADFUL_DB.MAPLEMONK.GLADFUL_sales_consolidated_pre_intermediate sc left join (select SMP.*, SM.productname, SM.Category, SM.Sub_category from (select replace(skucode,\'`\',\'\') skucode, replace(skucode_child,\'`\',\'\') skucode_child, qty, row_number() over (partition by skucode order by 1) rw from GLADFUL_DB.MAPLEMONK.sku_mapping_parent_child ) SMP left join (select * from (select replace(skucode, \'`\',\'\') skucode, productname, category, sub_category , row_number() over (partition by replace(skucode, \'`\',\'\') order by 1) rw from GLADFUL_DB.MAPLEMONK.sku_master) where rw=1 ) SM on SMP.skucode_child = SM.skucode where SMP.rw = 1 ) pcm on sc.sku_code = pcm.skucode ; create or replace table GLADFUL_DB.MAPLEMONK.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from GLADFUL_DB.MAPLEMONK.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from GLADFUL_DB.MAPLEMONK.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE GLADFUL_DB.MAPLEMONK.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM GLADFUL_DB.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM GLADFUL_DB.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE GLADFUL_DB.MAPLEMONK.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM GLADFUL_DB.MAPLEMONK. GLADFUL_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE GLADFUL_DB.MAPLEMONK.GLADFUL_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM GLADFUL_DB.MAPLEMONK.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GLADFUL_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        