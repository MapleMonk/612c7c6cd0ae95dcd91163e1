{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated_intermediate as select region ,b.customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,\'SHOPIFY\' as marketplace ,Upper(b.FINAL_UTM_CHANNEL) AS CHANNEL ,Upper(b.FINAL_UTM_SOURCE) AS SOURCE ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,b.shipping_status_update_date AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.ORDER_TIMESTAMP ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_INR AS DISCOUNT ,b.TAX_INR as TAX ,b.SHIPPING_PRICE_INR as SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,b.TOTAL_SALES_INR AS SELLING_PRICE_INR ,null as OMS_order_status ,UPPER(b.shipping_status) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status)) FINAL_SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,b.awb AWB ,upper(replace(parse_json(GATEWAY)[0],\'\"\',\'\')) PAYMENT_GATEWAY ,upper(case when replace(parse_json(GATEWAY)[0],\'\"\',\'\') like \'%cash%\' then \'COD\' when replace(parse_json(GATEWAY)[0],\'\"\',\'\') like \'%gift%\' then \'GIFT CARD\' else \'PREPAID\' end) Payment_Mode ,Upper(b.courier) AS COURIER ,b.Shipping_created_at AS DISPATCH_DATE ,case when b.shipping_status like \'delivered\' then b.shipping_status_update_date end AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,b.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(coalesce(shipmap.final_shipping_status,b.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(b.shipping_status_update_date)) when lower(coalesce(shipmap.final_shipping_status,b.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,COMMON_SKU as SKU_code ,UPPER(b.MAPPED_PRODUCT_NAME) PRODUCT_NAME_FINAL ,COMMON_SKU ,MAPPED_PRODUCT_NAME ,CATEGORY_1 PRODUCT_CATEGORY ,Category_2_Type_of_Tea ,Category_3_Type_of_Product ,Category_4_Pack_type ,Mother_SKU ,WEIGHT ,BRAND ,null WAREHOUSE ,b.pincode as pincode ,null as source_pincode from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_SHOPIFY_FACT_ITEMS b left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from VAHDAM_DB.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(b.shipping_status) = lower(ShipMap.shipping_status) union all select region ,Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON_SC\' as marketplace ,\'AMAZON_SC\' AS CHANNEL ,\'AMAZON_SC\' AS SOURCE ,afi.ORDER_ID ,afi.ORDER_ID reference_code ,Null as PHONE ,null as NAME ,null EMAIL ,null AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(afi.STATE) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP::date AS Order_Date ,afi.ORDER_TIMESTAMP ,afi.QUANTITY ,ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0) AS GROSS_SALES_BEFORE_TAX ,DISCOUNT_INR AS DISCOUNT ,afi.TAX_INR ,afi.SHIPPING_PRICE_INR ,TOTAL_SALES AS SELLING_PRICE ,TOTAL_SALES_INR AS SELLING_PRICE_inr ,null as OMS_order_status ,UPPER(afi.ORDER_STATUS) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,afi.ORDER_STATUS)) FINAL_SHIPPING_STATUS ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALEORDERITEMCODE ,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID) as SALES_ORDER_ITEM_ID ,NULL AWB ,NULL Payment_Gateway ,NULL Payment_Mode ,NULL AS COURIER ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,afi.ORDER_STATUS)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,afi.IS_REFUND AS RETURN_FLAG ,case when afi.is_refund = 1 then quantity::int end returned_quantity ,case when afi.is_refund = 1 then total_sales end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,null as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,COMMON_SKU as SKU_code ,UPPER(PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,COMMON_SKU ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,Category_2_Type_of_Tea ,Category_3_Type_of_Product ,Category_4_Pack_type ,Mother_SKU ,WEIGHT ,BRAND ,null WAREHOUSE ,pincode as pincode ,null as source_pincode from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_AMAZON_FACT_ITEMS AFI left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from VAHDAM_DB.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(afi.ORDER_STATUS) = lower(ShipMap.shipping_status) UNION ALL select region, NULL AS customer_id, \'AMAZON_1P\' AS shop_name, \'AMAZON_1P\' AS marketplace, \'AMAZON_1P\' AS channel, \'AMAZON_1P\' AS source, reference_code AS order_id, reference_code AS reference_code, NULL AS phone, NULL AS name, NULL AS email, NULL AS shipping_last_update_date, NULL AS sku, product_id, NULL AS product_name, currency, NULL AS city, NULL AS state, NULL AS order_status, NULL AS order_date, order_timestamp, quantity, line_item_sales_inr AS gross_sales_before_tax, NULL AS discount, NULL AS tax, NULL AS shipping_price, line_item_sales AS selling_price, line_item_sales_inr AS selling_price_inr, NULL AS oms_order_status, NULL AS shipping_status, NULL AS final_shipping_status, line_item_id AS saleorderitemcode, line_item_id AS sales_order_item_id, NULL AS awb, NULL AS payment_gateway, NULL AS payment_mode, NULL AS courier, NULL AS dispatch_date, NULL AS delivered_date, NULL AS delivered_status, NULL AS return_flag, NULL AS returned_quantity, NULL AS returned_sales, NULL AS cancelled_quantity, NULL AS new_customer_flag, NULL AS acquisition_product, NULL AS days_in_shipment, NULL AS acquisition_date, COMMON_SKU as SKU_code, UPPER(MAPPED_PRODUCT_NAME) PRODUCT_NAME_FINAL, COMMON_SKU, MAPPED_PRODUCT_NAME, CATEGORY_1 PRODUCT_CATEGORY, \"Category_2-Type_of_Tea\" Category_2_Type_of_Tea, \"Category_3-Type_of_Product\" Category_3_Type_of_Product , \"Category_4-Pack_type\" Category_4_Pack_type , \"Mother SKU\" Mother_SKU, WEIGHT, BRAND, null WAREHOUSE, null as pincode, null as source_pincode from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_Amazon_Vendor_central_Sales_consolidated ; create or replace table VAHDAM_DB.MAPLEMONK.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from VAHDAM_DB.MAPLEMONK.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from VAHDAM_DB.MAPLEMONK.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE VAHDAM_DB.MAPLEMONK.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM VAHDAM_DB.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM VAHDAM_DB.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE VAHDAM_DB.MAPLEMONK.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM VAHDAM_DB.MAPLEMONK.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; CREATE table if not exists VAHDAM_DB.MAPLEMONK.VAHDAM_DB_easyecom_returns_intermediate (ORDER_ID NUMBER,INVOICE_ID NUMBER,SUBORDER_ID VARIANT,REFERENCE_CODE VARCHAR,CREDIT_NOTE_ID NUMBER,ORDER_DATE TIMESTAMP_NTZ,INVOICE_DATE TIMESTAMP_NTZ,RETURN_DATE TIMESTAMP_NTZ,MANIFEST_DATE TIMESTAMP_NTZ,IMPORT_DATE TIMESTAMP_NTZ,LAST_UPDATE_DATE TIMESTAMP_NTZ,COMPANY_PRODUCT_ID VARIANT,PRODUCTNAME VARCHAR,PRODUCT_ID VARIANT,SKU VARCHAR,MARKETPLACE VARCHAR,MARKETPLACE_ID NUMBER,REPLACEMENT_ORDER NUMBER,RETURN_REASON VARCHAR,RETURNED_QUANTITY FLOAT,RETURN_AMOUNT_WITHOUT_TAX FLOAT,RETURN_TAX FLOAT,RETURN_SHIPPING_CHARGE FLOAT,RETURN_MISC FLOAT,TOTAL_RETURN_AMOUNT FLOAT) ; Create or replace table VAHDAM_DB.MAPLEMONK.VAHDAM_DB_easyecom_returns_fact_items as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_easyecom_returns_intermediate FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; CREATE table if not exists VAHDAM_DB.MAPLEMONK.VAHDAM_DB_UNICOMMERCE_RETURNS_INTERMEDIATE ( MARKETPLACE VARCHAR ,SOURCE VARCHAR ,ORDER_ID VARCHAR ,REFERENCE_CODE VARCHAR ,PHONE VARCHAR ,NAME VARCHAR ,EMAIL VARCHAR ,ORDER_DATE TIMESTAMP_NTZ ,RETURN_DISPLAYCODE VARCHAR ,RETURN_STATUS VARCHAR ,INVENTORY_RECEIVED_DATE TIMESTAMP_NTZ ,RETURN_COMPLETE_DATE TIMESTAMP_NTZ ,RETURN_INVOICE_DISPLAY_CODE VARCHAR ,RETURN_COURIER VARCHAR ,RETURN_PROVIDER_SHIPPING_STATUS VARCHAR ,RETURN_TRACKING_NUMBER VARCHAR ,RETURN_TYPE VARCHAR ,SALEORDERITEMCODE VARCHAR ,ITEMSKU VARCHAR ,ITEM_NAME VARCHAR ,INVENTORY_TYPE VARCHAR ); create or replace table VAHDAM_DB.MAPLEMONK.VAHDAM_DB_unicommerce_returns_fact_items as select ifnull(FE.channel,\'NA\') Marketing_CHANNEL ,FE.quantity RETURNED_QUANTITY ,FE.selling_price as TOTAL_RETURN_AMOUNT ,FE.tax as RETURN_TAX ,FE.selling_price - FE.tax as RETURN_AMOUNT_WITHOUT_TAX ,FR.* from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_UNICOMMERCE_RETURNS_INTERMEDIATE FR left join (select replace(reference_code,\'#\',\'\') REFERENCE_CODE, SALEORDERITEMCODE, channel, Source, sum(quantity) quantity, sum(selling_price) Selling_price, sum(tax) tax from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_sales_consolidated group by 1,2,3,4) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE and FR.saleOrderItemCode = FE.SALEORDERITEMCODE; create or replace table VAHDAM_DB.MAPLEMONK.VAHDAM_DB_RETURNS_CONSOLIDATED as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_easyecom_returns_fact_items group by 1,2,3 union all select upper(MARKETPLACE) Marketplace ,RETURN_COMPLETE_DATE RETURN_DATE ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_UNICOMMERCE_RETURNS_FACT_ITEMS group by 1,2,3;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from VAHDAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            