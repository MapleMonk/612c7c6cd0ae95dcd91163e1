{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE table if not exists rubans_db.MAPLEMONK.rubans_db_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, product_category varchar, product_sub_category varchar) ; create table if not exists rubans_db.MAPLEMONK.rubans_db_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar, awb varchar,Marketplace_LineItem_ID varchar, reference_code varchar,LAST_UPDATE_DATE date,PAYMENT_MODE varchar,COURIER varchar,MANIFEST_DATE date, DELIVERED_DATE date,mapped_product_name varchar,warehouse_name varchar,mapped_category varchar, mapped_sub_category varchar) ; create table if not exists rubans_db.MAPLEMONK.rubans_db_UNICOMMERCE_FACT_ITEMS ( order_id varchar ,city varchar ,state varchar ,saleorderitemcode varchar ,sales_order_item_id varchar ,shippingpackagecode varchar ,SHIPPINGPACKAGESTATUS varchar ,shipping_status varchar ,order_status varchar ,Courier varchar ,Dispatch_Date date ,Delivered_date date ,Return_flag int ,Return_quantity int ,cancelled_quantity int ,shipping_last_update_date date ,days_in_shipment float ,awb varchar ,marketplace varchar ,payment_method varchar ,PAYMENT_MODE varchar ,PRODUCT_ID varchar ,mapped_product_name varchar ,mapped_category varchar ,email varchar ,mapped_sub_category varchar) ; create or replace table rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate_pre as select b.customer_id::varchar customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.shop_name) as marketplace ,Upper(b.FINAL_UTM_CHANNEL) AS CHANNEL ,Upper(b.FINAL_UTM_SOURCE) AS SOURCE ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(d.shipping_status,b.shipping_status, c.shipping_status)) AS SHIPPING_STATUS ,UPPER(coalesce(Shipmap.final_shipping_status,d.shipping_status,b.shipping_status, c.shipping_status)) FINAL_SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,d.sales_order_item_id::varchar as SALES_ORDER_ITEM_ID ,coalesce(b.awb,c.awb,d.awb) AWB ,UPPER(b.GATEWAY) PAYMENT_GATEWAY ,upper(coalesce(b.payment_mode, c.payment_mode)) Payment_Mode ,Upper(coalesce(c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(d.dispatch_date,b.Shipping_created_at,c.manifest_date) AS DISPATCH_DATE ,coalesce(c.delivered_date,d.delivered_date,case when lower(FINAL_SHIPPING_STATUS) like \'delivered\' then b.shipping_status_update_date end) AS DELIVERED_DATE ,case when lower(FINAL_SHIPPING_STATUS) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_quantity,0) end returned_quantity ,case when coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(FINAL_SHIPPING_STATUS) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime))) when lower(FINAL_SHIPPING_STATUS) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,upper(d.warehouse_name) warehouse from rubans_db.Maplemonk.rubans_db_SHOPIFY_FACT_ITEMS b left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from rubans_db.Maplemonk.rubans_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%shopify%\') ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_mode payment_method ,email ,warehouse_name ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from rubans_db.Maplemonk.rubans_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=split_part(d.saleorderitemcode,\'-\',0) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from rubans_db.maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) = lower(ShipMap.shipping_status) union all select Null as customer_id ,upper(marketplace) shop_name ,upper(marketplace) marektplace ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,phone as PHONE ,name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.PRODUCT_ID ,PRODUCT_NAME AS PRODUCT_NAME ,CURRENCY ,upper(CITY) as city ,upper(STATE) AS State ,upper(ORDER_STATUS) order_status ,ORDER_DATE::date AS Order_Date ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0) - ifnull(tax,0) gross_sales_before_tax ,DISCOUNT AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_ORDER_STATUS ,upper(b.shipping_status) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status)) FINAL_SHIPPING_STATUS ,saleOrderItemCode as SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID as SALES_ORDER_ITEM_ID ,AWB ,null as payment_gateway ,payment_mode ,COURIER ,DISPATCH_DATE AS DISPATCH_DATE ,delivered_date as delivered_date ,case when upper(FINAL_SHIPPING_STATUS) in (\'DELIVERED\') then 1 end AS DELIVERED_STATUS ,return_flag AS RETURN_FLAG ,case when return_flag = 1 then suborder_quantity::int end returned_quantity ,case when return_flag = 1 then selling_price::float end returned_sales ,case when return_flag = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when order_status=\'COMPLETE\' then delivered_date::date-order_date::date else current_date - order_date::Date end as days_in_shipment ,NULL AS ACQUSITION_DATE ,sku_code ,upper(b.product_name_final) PRODUCT_NAME_FINAL ,upper(b.Product_Category) PRODUCT_CATEGORY ,upper(b.product_sub_category) PRODUCT_SUB_CATEGORY ,upper(warehouse_name) warehouse from rubans_db.MapleMonk.rubans_db_unicommerce_fact_items b left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from rubans_db.maplemonk.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) where not(lower(b.marketplace) like any (\'%shopify%\',\'%amazon%\',\'%amz%\')) union all Select NULL as Customer_ID ,upper(MSF.marketplace) shop_name ,upper(MSF.marketplace) marketplace ,upper(MSF.marketplace) Channel ,upper(MSF.marketplace) Sourcce ,MSF.STORE_ORDER_ID ORDER_ID ,MSF.ORDER_ID_FK ORDER_NAME ,NULL as PHONE ,NULL as NAME ,NULL as EMAIL ,coalesce(MSF.DELIVERED_ON,MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE,MSF.CANCELLED_ON,MSF.SHIPPED_ON,MSF.PACKED_ON,MSF.LOST_DATE) SHIPPING_LAST_UPDATE_DATE ,MSF.SKU_ID ,MSF.SKU_ID ,upper(MSF.STYLE_NAME) PRODUCT_NAME ,\'INR\' as Currency ,upper(MSF.CITY) City ,upper(MSF.STATE) State ,upper(MSF.ORDER_STATUS) Order_status ,MSF.created_on ,1 as quantity ,MSF.final_amount ,MSF.DISCOUNT ,MSF.TAX_RECOVERY ,MSF.SHIPPING_CHARGE ,MSF.final_amount ,NULL AS OMS_ORDER_STATUS ,upper(MSF.ORDER_STATUS) Shipping_status ,upper(MSF.ORDER_STATUS) Final_Shipping_status ,MSF.ORDER_LINE_ID ,MSF.ORDER_LINE_ID ,MSF.ORDER_TRACKING_NUMBER ,\'Myntra SJIT\' Payment_Gateway ,\'Myntra SJIT\' Payment_Mode ,\'Myntra\' Courier ,MSF.shipped_on ,MSF.DELIVERED_ON ,case when MSF.delivered_on is not null then 1 else 0 end AS DELIVERED_STATUS ,case when coalesce(MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE) is null then 0 else 1 end AS RETURN_FLAG ,case when coalesce(MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE) is null then 0 else 1 end returned_sales ,case when coalesce(MSF.RETURN_CREATION_DATE,MSF.RTO_CREATION_DATE) is null then 0 else ifnull(MSF.final_amount,0) end returned_sales ,case when MSF.cancelled_on is null then 0 else 1 end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISTION_PRODUCT ,case when MSF.delivered_on is not null then datediff(day,date(MSF.created_on),date(MSF.delivered_on)) when coalesce(MSF.delivered_on,MSF.return_creation_date,MSF.cancelled_on,MSF.lost_date,MSF.RTO_CREATION_DATE) is null then datediff(day,date(MSF.created_on), getdate()) end::int as Days_in_Shipment ,NULL as ACQUISTION_DATE ,MSF.SKU_ID ,upper(MSF.STYLE_NAME) PRODUCT_NAME_FINAL ,Upper(MSF.ARTICLE_TYPE) PRODUCT_CATEGORY ,Upper(MSF.ARTICLE_TYPE) PRODUCT_SUB_CATEGORY ,SELLER_WAREHOUSE_ID from rubans_db.maplemonk.Rubans_Myntra_SJIT_Fact_Items MSF union all select NULL as Customer_ID ,upper(VF.marketplace) shop_name ,upper(VF.marketplace) marketplace ,upper(VF.marketplace) Channel ,upper(VF.marketplace) Sourcce ,VF.ORDER_ID ,VF.ORDER_ID ORDER_NAME ,NULL as PHONE ,NULL as NAME ,NULL as EMAIL ,VF.INVOICE_DATE ,VF.SKU ,VF.SKU ,upper(VF.category) PRODUCT_NAME ,\'INR\' as Currency , NULL as CITY ,NULL as State ,\'DELIVERED\' as Order_Status ,VF.INVOICE_DATE ,VF.quantity ,VF.selling_price ,VF.MRP_Discount ,VF.GST_FINAL*VF.SELLING_PRICE ,0 as SHIPPING_CHARGE ,VF.SELLING_PRICE ,NULL AS OMS_ORDER_STATUS ,\'DELIVERED\' as Shipping_Status ,\'DELIVERED\' as Final_Shipping_Status ,VF.ORDER_LINE_ID ,VF.ORDER_LINE_ID , NULL AS TRACKING_NUMBER ,\'Myntra Vendor Flex\' Payment_Gateway ,\'Myntra Vendor Flex\' Payment_Mode ,\'Myntra\' Courier ,NULL as DISPATCH_DATE ,NULL AS DELIVERED_DATE ,1 Delivered_Status ,0 as return_flag ,0 as returned_quantity ,0 as returned_sales ,0 as cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISTION_PRODUCT ,NULL as DAYS_IN_SHIPMENT ,NULL as ACQUISTION_DATE ,VF.SKU ,NULL ,NULL ,NULL ,NULL from rubans_db.maplemonk.rubans_myntra_vendor_flex_Fact_Items VF ; create or replace table rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate as with SKU_Fresh_Flag as ( select SKU ,min(order_date) First_Sale_Date ,datediff(day, First_Sale_Date, current_date()) Days_Since_First_Sale ,case when Days_Since_First_Sale < 120 then \'Fresh SKU\' else \'Old SKU\' end SKU_Fresh_Flag from rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate_pre group by SKU) select SCIP.* ,SFF.SKU_FRESH_FLAG ,SFF.First_Sale_Date SKU_FIRST_SALE_DATE from rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate_pre SCIP left join SKU_Fresh_Flag SFF on SCIP.SKU = SFF.SKU ; create or replace table rubans_db.MAPLEMONK.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table rubans_db.MAPLEMONK.rubans_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from rubans_db.MAPLEMONK.rubans_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from rubans_db.MAPLEMONK.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from rubans_db.MAPLEMONK.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE rubans_db.MAPLEMONK.rubans_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM rubans_db.MAPLEMONK.rubans_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM rubans_db.MAPLEMONK.rubans_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE rubans_db.MAPLEMONK.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM rubans_db.MAPLEMONK.rubans_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM rubans_db.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM rubans_db.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE rubans_db.MAPLEMONK.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM rubans_db.MAPLEMONK.rubans_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE rubans_db.MAPLEMONK.rubans_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM rubans_db.MAPLEMONK.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; create or replace table rubans_db.MAPLEMONK.rubans_db_unicommerce_returns_detailed as select a.order_date ,a.reference_code ,b.channel marketing_channel ,case when lower(a.marketplace) like \'%shopify%\' then \'Shopify_rubansaccessories\' when lower(a.marketplace) like \'%amazon%\' then \'Amazon\' else lower(a.marketplace) end as marketplace ,sum(a.return_quantity) RETURN_QUANTITY ,sum(a.return_sales) RETURN_sales ,sum(case when return_flag = 1 then TAX end) TOTAL_RETURN_TAX ,sum(return_sales) - TOTAL_RETURN_TAX as TOTAL_RETURN_AMOUNT_EXCL_TAX from rubans_db.Maplemonk.rubans_db_unicommerce_fact_items a left join (select distinct reference_code, channel from rubans_db.Maplemonk.rubans_db_sales_consolidated) b on lower(replace(a.reference_code,\'#\',\'\')) = lower(replace(b.reference_code,\'#\',\'\')) group by 1,2,3,4; create or replace table rubans_db.MAPLEMONK.rubans_db_RETURNS_CONSOLIDATED as select upper(marketplace) Marketplace ,upper(marketing_channel) marketing_channel ,order_date ,sum(RETURN_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(RETURN_sales) TOTAL_RETURN_AMOUNT ,sum(TOTAL_RETURN_TAX) TOTAL_RETURN_TAX ,TOTAL_RETURN_AMOUNT - sum(TOTAL_RETURN_TAX) as TOTAL_RETURN_AMOUNT_EXCL_TAX from rubans_db.Maplemonk.rubans_db_unicommerce_returns_detailed group by 1,2,3 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from rubans_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        