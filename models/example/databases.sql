{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated_intermediate as select Null as customer_id ,upper(afi.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,case when lower(AFI.fulfillment_channel) = \'merchant\' then \'AMAZON SELFSHIP\' else \'AMAZON SMART CONNECT\' end AS SOURCE ,afi.ORDER_ID ,afi.ORDER_ID reference_code ,Null as PHONE ,BUYER_NAME NAME ,EEFI.EMAIL AS EMAIL ,EEFI.shipping_last_update_date::datetime AS SHIPPING_LAST_UPDATE_DATE ,afi.SKU ,afi.common_sku_code ,afi.PRODUCT_ID ,afi.PRODUCT_NAME ,afi.CURRENCY ,Upper(afi.CITY) CITY ,UPPER(afi.STATE) AS State ,UPPER(afi.ORDER_STATUS) Order_Status ,afi.ORDER_TIMESTAMP AS Order_Date ,div0(afi.QUANTITY, count(1) over (partition by afi.ORDER_ID, afi.SKU)) as PRE_QUANTITY ,div0(afi.quantity, count(1) over (partition by afi.ORDER_ID, afi.SKU)) * ifnull(\"Item Quantity Multiplier\",1) as QUANTITY ,div0((ifnull(TOTAL_SALES,0)-ifnull(afi.tax,0)+ifnull(DISCOUNT_BEFORE_TAX,0)) , count(1) over (partition by afi.ORDER_ID, afi.SKU)) AS GROSS_SALES_BEFORE_TAX ,div0(coalesce(mrp_map.mrp * afi.quantity,EEFI.mrp_sales,TOTAL_SALES), count(1) over (partition by afi.ORDER_ID, afi.SKU)) as total_mrp ,coalesce(total_mrp-div0(total_sales, count(1) over (partition by afi.ORDER_ID, afi.SKU) ),div0(DISCOUNT_BEFORE_TAX,count(1) over (partition by afi.ORDER_ID, afi.SKU, EEFI.Marketplace_LineItem_ID))) AS DISCOUNT ,div0(coalesce ((TOTAL_SALES - div0(TOTAL_SALES,1+div0(mrp_map.gst,100))),afi.TAX), count(1) over (partition by afi.ORDER_ID, afi.SKU)) as TAX ,div0(afi.SHIPPING_PRICE, count(1) over (partition by afi.ORDER_ID, afi.SKU)) SHIPPING_PRICE ,div0(TOTAL_SALES, count(1) over (partition by afi.ORDER_ID, afi.SKU)) AS SELLING_PRICE ,upper(EEFI.order_status) as OMS_order_status ,upper(EEFI.shipping_status) AS SHIPPING_STATUS ,upper(case when lower(shipmap.final_shipping_status) like \'%cancel%\' or lower(EEFI.shipping_status) like \'%cancel%\' or lower(afi.ORDER_STATUS) like \'%cancel%\' then \'cancelled\' else coalesce(shipmap.final_shipping_status,EEFI.shipping_status) end) FINAL_SHIPPING_STATUS ,coalesce(AFI.line_item_id, EEFI.Marketplace_LineItem_ID,concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID)) as SALEORDERITEMCODE ,coalesce(AFI.line_item_id, concat(afi.ORDER_ID,\'-\',afi.PRODUCT_ID)) as SALES_ORDER_ITEM_ID ,EEFI.awb AWB ,NULL Payment_Gateway ,upper(EEFI.payment_mode) Payment_Mode ,Upper(EEFI.Courier) AS COURIER ,EEFI.manifest_date AS DISPATCH_DATE ,EEFI.delivered_date AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,case when AFI.IS_REFUND=1 or rt.order_id1 is not null then 1 end AS RETURN_FLAG ,case when RETURN_FLAG = 1 then div0(AFI.quantity::int,count(1) over (partition by afi.ORDER_ID, afi.SKU)) end returned_quantity ,case when RETURN_FLAG=1 then div0(ifnull(coalesce(TOTAL_RETURN_AMOUNT,total_sales),0),count(1) over (partition by afi.ORDER_ID, afi.SKU)) end returned_sales ,case when afi.is_refund = 0 and lower(afi.order_status) in (\'cancelled\') then div0(AFI.quantity::int,count(1) over (partition by afi.ORDER_ID, afi.SKU)) end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce(shipmap.final_shipping_status,EEFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(afi.ORDER_TIMESTAMP),date(eefi.shipping_last_update_date::datetime)) when lower(coalesce(shipmap.final_shipping_status,EEFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(afi.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,coalesce(afi.SKU,eefi.SKU) as SKU_CODE ,UPPER(AFI.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,upper(EEFI.warehouse_name) WAREHOUSE ,coalesce(EEFI.PIN_CODE,afi.fulfillment_channel) Destination_Pincode ,coalesce(EEFI.PICKUP_PIN_CODE,fulfillment_channel) SOURCE_PINCODE ,EEFI.PAYMENT_GATEWAY_TRANSACTION_NUMBER ,EEFI.suborder_id ,\'1\' as ORDER_INCLUSION_FLAG from (select * from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_AMAZON_FACT_ITEMS where line_item_id is null) AFI left join (select * from ( select * ,row_number()over(partition by reference_code, lower(sku), lower(marketplace_sku), awb order by last_update_date desc) rw from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\',\'%gofynd%\') ) EEFI on AFI.Order_id = EEFI.reference_code and (lower(AFI.SKU) = lower(EEFI.MARKETPLACE_SKU) or lower(AFI.common_sku_code) = lower(EEFI.SKU)) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(EEFI.shipping_status,afi.ORDER_STATUS)) = lower(ShipMap.shipping_status) left join ( select * from ( select *, row_number() over(partition by sku_code order by end_date) rw from sleepycat_db.maplemonk.sku_mrp_cogs ) where rw=1 )mrp_map on lower(AFI.common_sku_code) = lower(mrp_map.sku_code) left join ( select sku as sku1 , reference_code as order_id1 , sum(TOTAL_RETURN_AMOUNT) as TOTAL_RETURN_AMOUNT from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_easyecom_returns_intermediate group by 1,2 )rt on AFI.ORDER_ID::varchar = rt.ORDER_ID1::varchar and lower(rt.sku1) = lower(AFI.common_sku_code) union all select Null as customer_id ,upper(AFI2.SHOP_NAME) Shop_name ,\'AMAZON\' as marketplace ,\'AMAZON\' AS CHANNEL ,case when lower(AFI2.fulfillment_channel) = \'merchant\' then \'AMAZON SELFSHIP\' else \'AMAZON SMART CONNECT\' end AS SOURCE ,AFI2.ORDER_ID ,AFI2.ORDER_ID reference_code ,Null as PHONE ,BUYER_NAME NAME ,EEFI.EMAIL AS EMAIL ,EEFI.shipping_last_update_date::datetime AS SHIPPING_LAST_UPDATE_DATE ,AFI2.SKU ,AFI2.common_sku_code ,AFI2.PRODUCT_ID ,AFI2.PRODUCT_NAME ,AFI2.CURRENCY ,Upper(AFI2.CITY) CITY ,UPPER(AFI2.STATE) AS State ,UPPER(AFI2.ORDER_STATUS) Order_Status ,AFI2.ORDER_TIMESTAMP AS Order_Date ,div0(AFI2.QUANTITY,count(1) over(partition by AFI2.ORDER_ID,coalesce(AFI2.line_item_id, EEFI.updated_marketplace_lineitem_id,concat(AFI2.ORDER_ID,\'-\',AFI2.PRODUCT_ID)))) as PRE_QUANTITY ,PRE_QUANTITY * ifnull(\"Item Quantity Multiplier\",1) as QUANTITY ,(ifnull(AFI2.TOTAL_SALES,0)-ifnull(AFI2.tax,0)+ifnull(AFI2.DISCOUNT_BEFORE_TAX,0)) AS GROSS_SALES_BEFORE_TAX ,coalesce(mrp_map.mrp * AFI2.quantity,EEFI.mrp_sales,AFI2.TOTAL_SALES) as total_mrp ,coalesce(total_mrp-AFI2.total_sales,AFI2.DISCOUNT_BEFORE_TAX) AS DISCOUNT ,coalesce ((AFI2.TOTAL_SALES - div0(AFI2.TOTAL_SALES,1+div0(mrp_map.gst,100))),AFI2.TAX) as TAX ,AFI2.SHIPPING_PRICE SHIPPING_PRICE ,AFI2.TOTAL_SALES AS SELLING_PRICE ,upper(EEFI.order_status) as OMS_order_status ,upper(EEFI.shipping_status) AS SHIPPING_STATUS ,upper(case when lower(shipmap.final_shipping_status) like \'%cancel%\' or lower(EEFI.shipping_status) like \'%cancel%\' or lower(AFI2.ORDER_STATUS) like \'%cancel%\' then \'cancelled\' else coalesce(shipmap.final_shipping_status,EEFI.shipping_status) end) FINAL_SHIPPING_STATUS ,coalesce(AFI2.line_item_id, EEFI.updated_marketplace_lineitem_id,concat(AFI2.ORDER_ID,\'-\',AFI2.PRODUCT_ID)) as SALEORDERITEMCODE ,coalesce(AFI2.line_item_id,concat(AFI2.ORDER_ID,\'-\',AFI2.PRODUCT_ID)) as SALES_ORDER_ITEM_ID ,EEFI.awb AWB ,NULL Payment_Gateway ,upper(EEFI.payment_mode) Payment_Mode ,Upper(EEFI.Courier) AS COURIER ,EEFI.manifest_date AS DISPATCH_DATE ,EEFI.delivered_date AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status, eefi.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,case when AFI2.IS_REFUND=1 or rt.order_id1 is not null then 1 end AS RETURN_FLAG ,case when RETURN_FLAG = 1 then div0(AFI2.quantity::int,count(1) over (partition by AFI2.ORDER_ID, AFI2.SKU)) end returned_quantity ,case when RETURN_FLAG=1 then div0(ifnull(coalesce(TOTAL_RETURN_AMOUNT,AFI2.total_sales),0),count(1) over (partition by AFI2.ORDER_ID, AFI2.SKU)) end returned_sales ,case when AFI2.is_refund = 0 and lower(AFI2.order_status) in (\'cancelled\') then AFI2.quantity::int end cancelled_quantity ,NULL as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,case when lower(coalesce(shipmap.final_shipping_status,EEFI.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(AFI2.ORDER_TIMESTAMP),date(eefi.shipping_last_update_date::datetime)) when lower(coalesce(shipmap.final_shipping_status,EEFI.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(AFI2.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,NULL AS ACQUSITION_DATE ,coalesce(AFI2.SKU,eefi.SKU) as SKU_CODE ,UPPER(AFI2.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(AFI2.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(AFI2.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,upper(EEFI.warehouse_name) WAREHOUSE ,coalesce(EEFI.PIN_CODE,AFI2.fulfillment_channel) Destination_Pincode ,coalesce(EEFI.PICKUP_PIN_CODE,fulfillment_channel) SOURCE_PINCODE ,EEFI.PAYMENT_GATEWAY_TRANSACTION_NUMBER ,EEFI.suborder_id ,\'1\' as ORDER_INCLUSION_FLAG from (select * from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_AMAZON_FACT_ITEMS where line_item_id is not null ) AFI2 left join ( select *, replace(split(marketplace_lineitem_id,\'|\')[0],\'\"\',\'\') as updated_marketplace_lineitem_id from ( select * ,row_number()over(partition by reference_code, marketplace_lineitem_id order by last_update_date desc) rw from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) like any (\'%amazon%\',\'%gofynd%\') ) EEFI on AFI2.Order_id = EEFI.reference_code and lower(replace(AFI2.line_item_id,\'\"\',\'\')) = lower(EEFI.updated_marketplace_lineitem_id) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(EEFI.shipping_status,AFI2.ORDER_STATUS)) = lower(ShipMap.shipping_status) left join ( select * from ( select *, row_number() over(partition by sku_code order by end_date) rw from sleepycat_db.maplemonk.sku_mrp_cogs ) where rw=1 )mrp_map on lower(AFI2.common_sku_code) = lower(mrp_map.sku_code) left join ( select sku as sku1 , reference_code as order_id1 , sum(TOTAL_RETURN_AMOUNT) as TOTAL_RETURN_AMOUNT from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_easyecom_returns_intermediate group by 1,2 )rt on AFI2.ORDER_ID::varchar = rt.ORDER_ID1::varchar and lower(rt.sku1) = lower(AFI2.common_sku_code) union all select Null as customer_id ,upper(SHOP_NAME) as SHOP_NAME ,upper(coalesce(upper(m_s.channel),case when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'P%\',\'%$P%\') then \'PEPPERFRY\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$STOWS%\') then \'Wooden Street\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$WS%\') then \'Wooden Street\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$B2B%\') then \'B2B\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$AS%\') then \'B2B\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$TEST%\') then \'TEST\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$C%\') then \'COVER\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$MKT%\') then \'Marketing\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$WKT%\') then \'Marketing\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$COMP%\') then \'Marketing\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any (\'%$SWST%\') then \'Springwell\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any(\'E%\',\'%$E%\') then \'EXCHANGE\' when UPPER(replace(b.reference_code,\'_\',\'$\')) like any(\'R%\',\'%$R%\') then \'REPLACEMENT\' when UPPER(reference_code) like \'B2B%\' then \'B2B\' when UPPER(reference_code) like \'TEST%\' then \'TEST\' when UPPER(reference_code) like any (\'C%\',\'COMP%\', \'MKT%\') then \'MARKETING\' when UPPER(reference_code) like \'WKT%\' then \'WELCOME KIT\' when REGEXP_LIKE(UPPER(reference_code), \'(.*_E|.*_E1|.*_E2|.*_E3|.*_E4|.*_E5)\') then \'EXCHANGE\' when REGEXP_SUBSTR(UPPER(reference_code), \'^(.*_R.*|.*_RPL.*)$\') IS NOT NULL then \'REPLACEMENT\' when REGEXP_LIKE(UPPER(reference_code), \'(.*_C|.*_C1|.*_C2|.*_C3|.*_CP|.*_COVER)\') then \'MARKETING\' when upper(reference_code) like any (\'MKT%\',\'INV%\',\'%SAM%\') then \'MARKETING\' when REGEXP_LIKE(UPPER(reference_code), \'(.*_CP|.*_COMP)\') and ifnull(selling_price,0) < 10 then \'MARKETING\' when UPPER(reference_code) like any (\'WS%\',\'STOWS%\') then \'WOODEN STREET\' when UPPER(reference_code) like \'PPF%\' or (lower(marketplace) like \'%offline%\' and lower(reference_code) like any (\'309%\') and length(reference_code) >= 9 and length(reference_code) <= 11) then \'PEPPERFRY\' else upper(marketplace) end)) as marketplace ,upper(coalesce(wfi.utm_mapped_channel,ga.ga_channel,b.marketplace)) AS CHANNEL ,upper(coalesce(wfi.utm_mapped_source,ga.ga_source, b.marketplace)) AS SOURCE ,b.ORDER_ID ,reference_code ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,SKU ,b.common_skucode ,PRODUCT_ID ,upper(PRODUCTNAME) AS PRODUCT_NAME ,CURRENCY ,upper(CITY) City ,upper(STATE) AS State ,upper(ORDER_STATUS) as Order_Status ,ORDER_DATE AS Order_Date ,SUBORDER_QUANTITY AS PRE_QUANTITY ,SUBORDER_QUANTITY * ifnull(\"Item Quantity Multiplier\",1) as QUANTITY ,ifnull(SELLING_PRICE,0)-ifnull(tax,0)+ifnull(DISCOUNT,0) AS GROSS_SALES_BEFORE_TAX ,coalesce(mrp_map.mrp * SUBORDER_QUANTITY,b.mrp_sales,b.SELLING_PRICE)as total_mrp ,coalesce ((total_mrp-SELLING_PRICE),DISCOUNT) AS DISCOUNT ,coalesce ((SELLING_PRICE - div0(SELLING_PRICE,1+div0(mrp_map.gst,100))),TAX) as TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_Order_Status ,upper(b.Shipping_status) AS SHIPPING_STATUS ,upper(case when lower(shipmap.final_shipping_status) like \'%cancel%\' or lower(b.shipping_status) like \'%cancel%\' or lower(ORDER_STATUS) like \'%cancel%\' then \'cancelled\' else coalesce(shipmap.final_shipping_status,b.shipping_status) end) FINAL_SHIPPING_STATUS ,Marketplace_LineItem_ID as SALEORDERITEMCODE ,suborder_id as SALES_ORDER_ITEM_ID ,AWB ,upper(wfi.payment_gateway) Payment_Gateway ,upper(coalesce(b.payment_mode,wfi.payment_method)) Payment_Mode ,UPPER(COURIER) COURIER ,MANIFEST_DATE as DISPATCH_DATE ,DELIVERED_DATE ,case when lower(coalesce(ShipMap.shipping_status,b.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,case when IS_REFUND=1 or rt.reference_code1 is not null then 1 end RETURN_FLAG ,case when RETURN_FLAG = 1 then suborder_quantity::int end returned_quantity ,case when RETURN_FLAG = 1 and lower(order_status) not in (\'cancelled\') then ifnull(coalesce(rt.TOTAL_RETURN_AMOUNT,SELLING_PRICE),0) end returned_sales ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,Days_in_shipment AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,SKU as SKU_CODE ,upper(mapped_product_name) as PRODUCT_NAME_FINAL ,upper(mapped_category) as PRODUCT_CATEGORY ,upper(mapped_sub_category) as PRODUCT_SUB_CATEGORY ,upper(WAREHOUSE_NAME) WAREHOUSE ,pin_code Destination_Pincode ,pickup_pin_code SOURCE_PINCODE ,PAYMENT_GATEWAY_TRANSACTION_NUMBER ,Suborder_id ,case when AOVE.order_id is not null then \'1\' else \'0\' end ORDER_INCLUSION_FLAG from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_EasyEcom_FACT_ITEMS b left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status,b.order_status)) = lower(ShipMap.shipping_status) left join ( select * from ( select transactionid ,upper(channel) ga_channel ,upper(final_source) ga_source ,row_number() over (partition by transactionid order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_GA_ORDER_BY_SOURCE_CONSOLIDATED ) where rw = 1 ) ga on b.reference_code = ga.transactionid LEFT JOIN ( select * from (select order_id as id1,utm_medium, utm_source, utm_campaign, utm_content, utm_path, utm_mapped_source, utm_mapped_channel, payment_gateway, payment_method, row_number() over (partition by order_id order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.SleepyCat_DB_Woocommerce_FACT_ITEMS ) where rw=1 )wfi on wfi.id1 = b.reference_code left join ( select * from ( select *, row_number() over(partition by sku_code order by end_date) rw from sleepycat_db.maplemonk.sku_mrp_cogs ) where rw=1 )mrp_map on lower(b.sku) = lower(mrp_map.sku_code) left join ( select suborder_id as suborder_id1 , reference_code as reference_code1 , sum(TOTAL_RETURN_AMOUNT) as TOTAL_RETURN_AMOUNT from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_easyecom_returns_intermediate group by 1,2 )rt on b.reference_code = rt.reference_code1 and rt.suborder_id1 = b.suborder_id left join (select order_id from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_EasyEcom_FACT_ITEMS GROUP BY ORDER_ID HAVING sum(SELLING_PRICE) > 50 ) AOVE on b.order_id = AOVE.order_id left join ( select * from ( select *,row_number() over(partition by suffix order by 1)rw from sleepycat_db.maplemonk.marketplace_suffix ) where rw=1 )m_s on lower(replace(b.reference_code,\'_\',\'$\')) like \'%\' || lower(replace(m_s.suffix,\'_\',\'$\')) where not(lower(b.marketplace) like any (\'%amazon%\',\'%gofynd%\')) AND (lower(B.customer_name) not in (select lower(STN_Customer_Name) from sleepycat_db.maplemonk.stock_transfer_customers ) ) AND not(reference_code like \'STN%\') ; create or replace table SLEEPYCAT_DB.MAPLEMONK.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from SLEEPYCAT_DB.MAPLEMONK.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from SLEEPYCAT_DB.MAPLEMONK.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE SLEEPYCAT_DB.MAPLEMONK.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM SLEEPYCAT_DB.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM SLEEPYCAT_DB.MAPLEMONK.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE SLEEPYCAT_DB.MAPLEMONK.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM SLEEPYCAT_DB.MAPLEMONK.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; UPDATE SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated AS A SET A.marketplace = B.marketplace FROM ( SELECT distinct reference_code ,marketplace FROM SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated )B WHERE SPLIT_PART(A.reference_code,\'_\',1) = B.reference_code and lower(A.marketplace) like any (\'%exchange%\',\'%replacement%\',\'%offline%\') ; Create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_easyecom_returns_fact_items as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_easyecom_returns_intermediate FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_sales_consolidated) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_RETURNS_CONSOLIDATED as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_easyecom_returns_fact_items where credit_note_number <> \'0\' group by 1,2,3 order by 2 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        