{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE table if not exists prd_db.beardo.dwh_AMAZON_FACT_ITEMS ( Customer_id varchar,Shop_name varchar,Source varchar, order_id varchar, order_name varchar, phone varchar, name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, product_name varchar, currency varchar, city varchar, state varchar, order_status varchar, order_timestamp varchar, shipping_price float, quantity float, discount_before_tax float, tax float, total_Sales float, is_refund number(38,0), product_name_final varchar, category varchar, product_sub_category varchar) ; create table if not exists prd_db.beardo.dwh_EasyEcom_FACT_ITEMS ( customer_id varchar, Shop_name varchar,marketplace varchar,Source varchar, order_id varchar, contact_num varchar, customer_name varchar, email varchar, shipping_last_update_date varchar, sku varchar, product_id varchar, productname varchar, currency varchar, city varchar, state varchar, order_status varchar, order_Date varchar, shipping_price float, suborder_quantity float, discount float, tax float, selling_price float, is_refund number(38,0), suborder_id variant, product_name_final varchar, product_category varchar, product_sub_category varchar, new_customer_flag varchar, shipping_status varchar, days_in_shipment varchar) ; create or replace table prd_db.beardo.dwh_sales_consolidated_intermediate as select b.customer_id ,UPPER(b.SHOP_NAME) as shop_name ,UPPER(b.FINAL_UTM_CHANNEL) AS CHANNEL ,UPPER(b.FINAL_UTM_SOURCE) AS SOURCE ,b.ORDER_ID ,b.Order_Name as reference_code ,coalesce(b.phone ,c.contact_num) as phone ,coalesce(b.name ,c.Customer_Name) as Customer_Name ,coalesce(b.email ,c.email) as email ,coalesce(b.SHOPIFY_SHIPPING_UPDATED_DATE, c.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,coalesce(b.sku ,c.sku) as sku ,b.PRODUCT_ID ,b.PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) City ,Upper(b.STATE) AS State ,upper(coalesce(c.order_status,b.order_status)) as Order_Status ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.SHIPPING_PRICE ,b.QUANTITY ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status)) AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,coalesce(c.invoice_id,b.LINE_ITEM_ID) as INVOICE_ID ,coalesce(c.courier ,b.SHOPIFY_COURIER) AS COURIER ,upper(coalesce(b.SHOPIFY_SHIPPING_STATUS,c.shipping_status)) AS SHIPPING_STATUS ,c.manifest_date::datetime AS DISPATCH_DATE ,case when upper(coalesce(b.SHOPIFY_SHIPPING_STATUS,c.shipping_status)) in (\'DELIVERED\') then 1 end AS DELIVERED_STATUS ,coalesce(c.IS_REFUND, b.IS_REFUND) AS RETURN_FLAG ,case when return_flag= 1 then quantity::int end returned_quantity ,case when return_flag = 0 and lower(coalesce(b.order_status,c.order_status)) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.NEW_CUSTOMER_FLAG::varchar new_customer_flag ,b.ACQUISITION_PRODUCT ,case when upper(coalesce(b.SHOPIFY_SHIPPING_STATUS,c.shipping_status)) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,coalesce(c.warehouse_name,\'NA\') as Warehouse_Name ,coalesce(s.Product_name_mapped,b.product_name,c.product_name) PRODUCT_NAME_FINAL ,coalesce(s.Product_Category_Mapped,c.category,b.category) PRODUCT_CATEGORY ,coalesce(s.Product_Sub_Category_Mapped,null) PRODUCT_SUB_CATEGORY ,null as GA_SOURCE ,null as GA_MEDIUM ,null as VIEW_ID ,c.payment_mode payment_mode ,b.discount_code ,b.affiliate from prd_db.beardo.DWH_SHOPIFY_FACT_ITEMS b left join (select * from ( select *,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from prd_db.beardo.dwh_unicommerce_fact_items ) z where z.rw = 1 ) c on lower(replace(b.order_name,\'#\',\'\')) = lower(c.reference_code) and b.order_timestamp::date = c.order_date::date left join (select * from (select distinct skucode, name Product_name_mapped, Category Product_category_mapped, Sub_category Product_Sub_Category_Mapped, row_number() over (partition by skucode order by 1) rw from prd_db.beardo.dwh_sku_master) where rw=1 ) S on lower(b.sku)=lower(s.skucode) union all select Null as customer_id ,\'AMAZON\' as SHOP_NAME ,\'AMAZON\' as CHANNEL ,\'AMAZON\' AS SOURCE ,b.ORDER_ID ,b.Order_ID as reference_code ,coalesce(b.phone ,c.contact_num) as phone ,coalesce(b.name ,c.Customer_Name) as Customer_Name ,coalesce(b.email ,c.email) as email ,c.shipping_last_update_date::datetime AS SHIPPING_LAST_UPDATE_DATE ,coalesce(b.sku ,c.sku) as sku ,b.PRODUCT_ID ,b.PRODUCT_NAME ,b.CURRENCY ,upper(b.CITY) as City ,upper(b.STATE) AS State ,upper(coalesce(c.order_status,b.order_status)) as Order_Status ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.SHIPPING_PRICE ,b.QUANTITY ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,coalesce(c.shipping_status ,NULL) AS SHIPPINGPACKAGESTATUS ,NULL as SALES_ORDER_ITEM_ID ,coalesce(c.invoice_id,null) as INVOICE_ID ,coalesce(c.courier ,NULL) AS COURIER ,upper(coalesce(c.shipping_status ,NULL)) AS SHIPPING_STATUS ,c.manifest_date::datetime AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,coalesce(c.IS_REFUND, b.IS_REFUND) AS RETURN_FLAG ,case when return_flag = 1 then quantity::int end returned_quantity ,case when return_flag = 0 and lower(coalesce(c.order_status,b.order_status)) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when upper(c.shipping_STATUS) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_TIMESTAMP),date(c.shipping_last_update_date::datetime)) else datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,coalesce(c.warehouse_name,\'NA\') as Warehouse_Name ,coalesce(s.Product_name_mapped,b.product_name,c.product_name) PRODUCT_NAME_FINAL ,coalesce(c.category,b.category) PRODUCT_CATEGORY ,null PRODUCT_SUB_CATEGORY ,\'Amazon\' as GA_SOURCE ,\'Amazon\' as GA_MEDIUM ,null as VIEW_ID ,c.payment_mode payment_mode ,null as discount_code ,null as affiliate from prd_db.beardo.dwh_AMAZON_FACT_ITEMS b left join (select * from ( select *,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from prd_db.beardo.DWH_UNICOMMERCE_FACT_ITEMS ) z where z.rw = 1 ) c on lower(replace(b.order_name,\'#\',\'\')) = lower(c.reference_code) and b.order_timestamp::date = c.order_date::date left join (select * from (select distinct skucode, name Product_name_mapped, Category Product_category_mapped, Sub_category Product_Sub_Category_Mapped, row_number() over (partition by skucode order by 1) rw from prd_db.beardo.dwh_sku_master) where rw=1 ) S on lower(b.SKU)=lower(s.skucode) union all select Null as customer_id ,upper(SHOP_NAME) shop_name ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,ORDER_ID ,reference_code ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.PRODUCT_ID ,PRODUCT_NAME AS PRODUCT_NAME ,CURRENCY ,upper(CITY) as city ,upper(STATE) AS State ,upper(ORDER_STATUS) order_status ,ORDER_DATE::date AS Order_Date ,SHIPPING_PRICE ,SUBORDER_QUANTITY AS QUANTITY ,DISCOUNT AS DISCOUNT ,TAX ,SELLING_PRICE AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,upper(shipping_status) AS SHIPPINGPACKAGESTATUS ,suborder_id as SALES_ORDER_ITEM_ID ,INVOICE_ID as INVOICE_ID ,COURIER ,upper(shipping_status) AS SHIPPING_STATUS ,Manifest_date AS DISPATCH_DATE ,case when upper(shipping_status) in (\'DELIVERED\') then 1 end AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then suborder_quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when upper(shipping_STATUS) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_DATE),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_DATE), getdate()) end::int AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,warehouse_name ,coalesce(s.Product_name_mapped,b.product_name) PRODUCT_NAME_FINAL ,coalesce(s.Product_Category_Mapped,b.category) PRODUCT_CATEGORY ,coalesce(s.Product_Sub_Category_Mapped,null) PRODUCT_SUB_CATEGORY ,Marketplace as GA_SOURCE ,Marketplace as GA_MEDIUM ,null as VIEW_ID ,payment_mode ,null as discount_code ,null as affiliate from prd_db.beardo.DWH_UNICOMMERCE_FACT_ITEMS b left join (select * from (select distinct skucode, name Product_name_mapped, Category Product_category_mapped, Sub_category Product_Sub_Category_Mapped, row_number() over (partition by skucode order by 1) rw from prd_db.beardo.dwh_sku_master) where rw=1 ) S on lower(b.sku)=lower(s.skucode) where lower(marketplace) not like (\'%shopify%\') and lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%amz%\'); create or replace table prd_db.beardo.dwh_Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from prd_db.beardo.dwh_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from prd_db.beardo.dwh_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL ; create or replace table prd_db.beardo.dwh_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from prd_db.beardo.dwh_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from prd_db.beardo.dwh_Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from prd_db.beardo.dwh_Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE prd_db.beardo.dwh_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE prd_db.beardo.dwh_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN acquisition_SKUCODE varchar(16777216); ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN acquisition_PRODUCT_SUB_CATEGORY varchar(16777216); ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN acquisition_PRODUCT_CATEGORY varchar(16777216); ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE prd_db.beardo.dwh_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE prd_db.beardo.dwh_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM prd_db.beardo.dwh_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE prd_db.beardo.dwh_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE prd_db.beardo.dwh_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM prd_db.beardo.dwh_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE prd_db.beardo.dwh_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; create or replace temporary table prd_db.beardo.dwh_temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER ( partition BY customer_id_final) firstOrderdate FROM prd_db.beardo.dwh_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE prd_db.beardo.dwh_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM prd_db.beardo.dwh_temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE prd_db.beardo.dwh_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM prd_db.beardo.dwh_temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE prd_db.beardo.dwh_temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, product_category, product_sub_category, sku, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, product_category, product_sub_category, sku, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM prd_db.beardo.dwh_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE prd_db.beardo.dwh_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM prd_db.beardo.dwh_temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE prd_db.beardo.dwh_sales_consolidated AS A SET A.acquisition_product_category=B.product_category FROM ( SELECT * FROM prd_db.beardo.dwh_temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE prd_db.beardo.dwh_sales_consolidated AS A SET A.acquisition_product_sub_category=B.product_sub_category FROM ( SELECT * FROM prd_db.beardo.dwh_temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE prd_db.beardo.dwh_sales_consolidated AS A SET A.acquisition_SKUCODE=B.SKU FROM ( SELECT * FROM prd_db.beardo.dwh_temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; create or replace table prd_db.beardo.dwh_Unicommerce_RETURNS_SUMMARY_Beardo as select upper(shop_name) Marketplace ,order_date ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(RETURN_sales) TOTAL_RETURN_AMOUNT ,sum(case when is_refund = 1 then TAX end) TOTAL_RETURN_TAX ,TOTAL_RETURN_AMOUNT - TOTAL_RETURN_TAX as TOTAL_RETURN_AMOUNT_EXCL_TAX from prd_db.beardo.dwh_unicommerce_fact_items group by 1,2 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from PRD_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        