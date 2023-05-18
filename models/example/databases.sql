{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE table if not exists Perfora_DB.MapleMonk.Perfora_DB_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, product_category varchar, product_sub_category varchar) ; create table if not exists Perfora_DB.MapleMonk.Perfora_DB_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar) ; create or replace table Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated_intermediate as select b.customer_id ,UPPER(b.SHOP_NAME) as shop_name ,UPPER(coalesce(GAC.channel,b.FINAL_UTM_CHANNEL)) AS SOURCE ,b.ORDER_ID ,b.Order_Name as reference_code ,coalesce(b.phone ,c.contact_num) as phone ,coalesce(b.name ,c.Customer_Name) as Customer_Name ,coalesce(b.email ,c.email) as email ,coalesce(b.SHOPIFY_SHIPPING_UPDATED_DATE, c.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,coalesce(b.sku ,c.sku) as sku ,b.PRODUCT_ID ,b.PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) City ,Upper(b.STATE) AS State ,coalesce(c.order_status,b.order_status) as Order_Status ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.SHIPPING_PRICE ,b.QUANTITY ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status) AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,coalesce(c.invoice_id,b.LINE_ITEM_ID) as INVOICE_ID ,coalesce(c.courier ,b.SHOPIFY_COURIER) AS COURIER ,coalesce(b.SHOPIFY_SHIPPING_STATUS,c.shipping_status) AS SHIPPING_STATUS ,c.manifest_date::datetime AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,coalesce(c.IS_REFUND, b.IS_REFUND) AS RETURN_FLAG ,case when return_flag= 1 then quantity::int end returned_quantity ,case when return_flag = 0 and lower(coalesce(c.order_status,b.order_status)) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.NEW_CUSTOMER_FLAG::varchar new_customer_flag ,ACQUISITION_PRODUCT ,case when shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) when shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) end::int AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,coalesce(c.warehouse_name,\'NA\') as Warehouse_Name ,coalesce(s.Product_name_mapped,b.product_name,c.productname) PRODUCT_NAME_FINAL ,coalesce(s.Product_Category_Mapped,c.category,b.category) PRODUCT_CATEGORY ,coalesce(s.Product_Sub_Category_Mapped,null) PRODUCT_SUB_CATEGORY ,GAC.GA_SOURCE ,GAC.GA_MEDIUM ,GAC.VIEW_ID ,c.payment_mode payment_mode from Perfora_DB.MapleMonk.Perfora_DB_SHOPIFY_FACT_ITEMS b left join (select * from ( select *,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from perfora_db.maplemonk.Perfora_DB_EasyEcom_FACT_ITEMS ) z where z.rw = 1 ) c on lower(replace(b.order_name,\'#\',\'\')) = lower(c.reference_code) and b.order_timestamp::date = c.order_date::date left join (select * from (select distinct skucode, sku_type, product_id,productname Product_name_mapped, Category Product_category_mapped, Sub_category Product_Sub_Category_Mapped, row_number() over (partition by product_id order by 1) rw from perfora_db.maplemonk.sku_master) where rw=1 ) S on lower(b.product_id)=lower(s.product_id) left join (select * from (select *, Row_number() OVER (partition BY ga_transactionid ORDER BY GA_DATE DESC) rw from ga_order_by_source_consolidated_perfora) where rw=1) GAC on lower(replace(b.order_name,\'#\',\'\')) =lower(replace(GAC.GA_TRANSACTIONID,\'#\',\'\')) union all select Null as customer_id ,\'AMAZON\' as SHOP_NAME ,\'AMAZON\' AS SOURCE ,b.ORDER_ID ,b.Order_ID as reference_code ,coalesce(b.phone ,c.contact_num) as phone ,coalesce(b.name ,c.Customer_Name) as Customer_Name ,coalesce(b.email ,c.email) as email ,coalesce(c.shipping_last_update_date::datetime, NULL) AS SHIPPING_LAST_UPDATE_DATE ,coalesce(b.sku ,c.sku) as sku ,b.PRODUCT_ID ,b.PRODUCT_NAME ,b.CURRENCY ,upper(b.CITY) as City ,upper(b.STATE) AS State ,coalesce(c.order_status,b.order_status) as Order_Status ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.SHIPPING_PRICE ,b.QUANTITY ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,coalesce(c.shipping_status ,NULL) AS SHIPPINGPACKAGESTATUS ,NULL as SALES_ORDER_ITEM_ID ,coalesce(c.invoice_id,null) as INVOICE_ID ,coalesce(c.courier ,NULL) AS COURIER ,coalesce(c.shipping_status ,NULL) AS SHIPPING_STATUS ,c.manifest_date::datetime AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,coalesce(c.IS_REFUND, b.IS_REFUND) AS RETURN_FLAG ,case when return_flag = 1 then quantity::int end returned_quantity ,case when return_flag = 0 and lower(coalesce(c.order_status,b.order_status)) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) when shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) end::int AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,coalesce(c.warehouse_name,\'NA\') as Warehouse_Name ,coalesce(s.Product_name_mapped,b.product_name,c.productname) PRODUCT_NAME_FINAL ,coalesce(s.Product_Category_Mapped,c.category,b.category) PRODUCT_CATEGORY ,coalesce(s.Product_Sub_Category_Mapped,null) PRODUCT_SUB_CATEGORY ,\'Amazon\' as GA_SOURCE ,\'Amazon\' as GA_MEDIUM ,null as VIEW_ID ,c.payment_mode payment_mode from Perfora_DB.MapleMonk.Perfora_DB_AMAZON_FACT_ITEMS b left join (select * from ( select *,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from perfora_db.maplemonk.Perfora_DB_EasyEcom_FACT_ITEMS ) z where z.rw = 1 ) c on lower(replace(b.order_name,\'#\',\'\')) = lower(c.reference_code) and b.order_timestamp::date = c.order_date::date left join (select * from (select distinct skucode, sku_type, product_id asin, productname Product_name_mapped, Category Product_category_mapped, Sub_category Product_Sub_Category_Mapped, row_number() over (partition by asin order by 1) rw from perfora_db.maplemonk.sku_master) where rw=1 ) S on lower(b.SKU)=lower(s.ASIN) union all select Null as customer_id ,upper(SHOP_NAME) shop_name ,upper(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.PRODUCT_ID ,PRODUCTNAME AS PRODUCT_NAME ,CURRENCY ,upper(CITY) as city ,upper(STATE) AS State ,ORDER_STATUS ,ORDER_DATE::date AS Order_Date ,SHIPPING_PRICE ,SUBORDER_QUANTITY AS QUANTITY ,DISCOUNT AS DISCOUNT ,TAX ,SELLING_PRICE AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,shipping_status AS SHIPPINGPACKAGESTATUS ,suborder_id as SALES_ORDER_ITEM_ID ,INVOICE_ID as INVOICE_ID ,COURIER ,shipping_status AS SHIPPING_STATUS ,Manifest_date AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then suborder_quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,Days_in_shipment AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,warehouse_name ,coalesce(s.Product_name_mapped,b.productname) PRODUCT_NAME_FINAL ,coalesce(s.Product_Category_Mapped,b.category) PRODUCT_CATEGORY ,coalesce(s.Product_Sub_Category_Mapped,null) PRODUCT_SUB_CATEGORY ,Marketplace as GA_SOURCE ,Marketplace as GA_MEDIUM ,null as VIEW_ID ,payment_mode from Perfora_DB.MapleMonk.Perfora_DB_EasyEcom_FACT_ITEMS b left join (select * from (select distinct skucode, product_id, sku_type, productname Product_name_mapped, Category Product_category_mapped, Sub_category Product_Sub_Category_Mapped, row_number() over (partition by product_id order by 1) rw from perfora_db.maplemonk.sku_master) where rw=1 ) S on lower(b.product_id)=lower(s.product_id) where lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%shopify%\') and lower(customer_name) not in (select lower(STN_Customer_Name) from Perfora_DB.MapleMonk.Perfora_Stock_Transfer_Customers); create or replace table Perfora_DB.MapleMonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from Perfora_DB.MapleMonk.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from Perfora_DB.MapleMonk.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN acquisition_SKUCODE varchar(16777216); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN acquisition_PRODUCT_SUB_CATEGORY varchar(16777216); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN acquisition_PRODUCT_CATEGORY varchar(16777216); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE Perfora_DB.MapleMonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER ( partition BY customer_id_final) firstOrderdate FROM Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM Perfora_DB.MapleMonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM Perfora_DB.MapleMonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE Perfora_DB.MapleMonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, product_category, product_sub_category, sku, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, product_category, product_sub_category, sku, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM Perfora_DB.MapleMonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS A SET A.acquisition_product_category=B.product_category FROM ( SELECT * FROM Perfora_DB.MapleMonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS A SET A.acquisition_product_sub_category=B.product_sub_category FROM ( SELECT * FROM Perfora_DB.MapleMonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE Perfora_DB.MapleMonk.Perfora_DB_sales_consolidated AS A SET A.acquisition_SKUCODE=B.SKU FROM ( SELECT * FROM Perfora_DB.MapleMonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; create or replace table Perfora_db.MAPLEMONK.fact_items_easyecom_returns_detailed_perfora as select ORDER_ID ,INVOICE_ID ,RI.VALUE:\"suborder_id\" SUBORDER_ID ,REFERENCE_CODE ,CREDIT_NOTE_ID ,try_to_timestamp(ORDER_DATE) ORDER_DATE ,try_to_timestamp(INVOICE_DATE) INVOICE_DATE ,try_to_timestamp(RETURN_DATE) RETURN_DATE ,try_to_timestamp(MANIFEST_DATE) MANIFEST_DATE ,try_to_timestamp(IMPORT_DATE) IMPORT_DATE ,try_to_timestamp(LAST_UPDATE_DATE) LAST_UPDATE_DATE ,RI.VALUE:company_product_id COMPANY_PRODUCT_ID ,replace(RI.VALUE:productName,\'\"\',\'\') PRODUCTNAME ,RI.VALUE:product_id PRODUCT_ID ,replace(RI.VALUE:sku,\'\"\',\'\') SKU ,MARKETPLACE ,MARKETPLACE_ID ,REPLACEMENT_ORDER ,replace(RI.VALUE:return_reason,\'\"\',\'\') RETURN_REASON ,ifnull(RI.VALUE:returned_item_quantity::float,0) RETURNED_QUANTITY ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) RETURN_AMOUNT_WITHOUT_TAX ,ifnull(RI.Value:credit_note_total_item_tax::float,0) RETURN_TAX ,ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0) RETURN_SHIPPING_CHARGE ,ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) RETURN_MISC ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) + ifnull(RI.Value:credit_note_total_item_tax::float,0) + ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0)+ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) TOTAL_RETURN_AMOUNT from perfora_db.MAPLEMONK.easyecom_easyecom___perfora_returns R, LATERAL flatten(INPUT => R.ITEMS) RI; Create or replace table perfora_db.MAPLEMONK.fact_items_easyecom_returns_detailed_perfora as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from perfora_db.MAPLEMONK.fact_items_easyecom_returns_detailed_perfora FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from perfora_db.MAPLEMONK.perfora_db_sales_consolidated) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table perfora_db.MAPLEMONK.EASYECOM_RETURNS_SUMMARY_Perfora as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from Perfora_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_perfora group by 1,2,3 order by 2 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        