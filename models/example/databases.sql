{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 as select case when upper(s.brand) like \'%THREE60PLUS%\' then \'THREE60PLUS\' when upper(s.brand) like \'%THREE60%\' then \'THREE60\' else upper(SHOP_NAME) end as temp_SHOP_NAME ,awb ,CARRIER_ID ,COURIER ,CUSTOMER_NAME ,EMAIL ,contact_num as phone ,MARKETPLACE ::varchar as MARKETPLACE ,MARKETPLACE_ID ,ORDER_ID ,SUBORDER_Id ::varchar as SUBORDER_Id ,INVOICE_ID :: varchar as INVOICE_ID ,REFERENCE_CODE ,MANIFEST_DATE ::timestamp as MANIFEST_DATE ,SHIPPING_LAST_UPDATE_DATE ::timestamp as SHIPPING_LAST_UPDATE_DATE ,upper(SHIPPING_STATUS::varchar) SHIPPING_STATUS ,b.SKU ::varchar as SKU ,b.SKU_TYPE :: varchar as SKU_TYPE ,b.PRODUCT_ID ::varchar as PRODUCT_ID ,b.PRODUCTNAME ::varchar as PRODUCTNAME ,CURRENCY ::varchar as CURRENCY ,IS_REFUND :: int as IS_REFUND ,CITY ,STATe ,ORDER_STATUS ,ORDER_DATE ::Date as order_date ,ORDER_DATE as order_timestamp ,SHIPPING_PRICE ,NUMBER_OF_PRODUCTS_IN_COMBO ,coalesce(number_of_products_in_combo,SUBORDER_QUANTITY*(s.product_quantity)) SUBORDER_QUANTITY ,SHIPPED_QUANTITY ,RETURNED_QUANTITY ,CANCELLED_QUANTITY ,RETURN_SALES ,CANCEL_SALES ,TAX ,SUBORDER_MRP ,upper(coalesce(s.Product_Category,b.category)) category ,upper(coalesce(s.Product_name,b.PRODUCTNAME)) Product_Name_Mapped ,upper(s.Report_Category) Report_Category ,upper(s.Product_Pack) Product_Pack ,s.product_quantity AS Product_quantity ,DISCOUNT ,SELLING_PRICE ,MRP_SALES ,DISCOUNT_MRP ,NEW_CUSTOMER_FLAG ,NEW_CUSTOMER_FLAG_MONTH ,WAREHOUSE_NAME ,case when upper(shipping_STATUS) in (\'RETURNED\', \'RTO\') and order_date::date >= \'2024-05-01\' then 1 else 0 end as return_flag ,case when upper(shipping_STATUS) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_DATE),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_DATE), getdate()) end::int AS DAYS_IN_SHIPMENT ,CHANNEL ::varchar as CHANNEL ,NULL as Shopify_UTM_source ,NULL as Shopify_UTM_Medium ,NULL AS UTM_CAMPAIGN ,PAYMENT_MODE ,IMPORT_DATE ,LAST_UPDATE_DATE ,invoice_date ,company_name ,b.pin_code ,NULL as FINAL_UTM_CAMPAIGN ,\'Synced\' as EasyEcom_Sync_Flag, case when cancel_sales is not null then LAST_UPDATE_DATE end as cancelled_at ,\'Three60\' as Data_Source from rpsg_DB.maplemonk.fact_items_easyecom_drv b left join ( select * from (select marketplace_sku,sku, brand , category as Product_Category, \"Product Name\" AS PRODUCT_NAME, \"Pack Size\"AS Product_Pack, null as Report_Category, null as product_quantity, row_number() over (partition by lower(sku) order by 1) rw from rpsg_DB.maplemonk.three60_sku_master where sku is not null and lower(marketplace) like \'%three60%\') where rw=1 ) S on lower(b.sku)=lower(s.sku) where (lower(b.marketplace) like \'%three60%\') UNION ALL select case when lower(product_name) like \'three60+%\' then \'THREE60PLUS\' ELSE \'THREE60\' END as temp_SHOP_NAME ,EEF.awb ,EEF.CARRIER_ID ,EEF.COURIER ,EEF.CUSTOMER_NAME ,EEF.EMAIL ,EEF.contact_num as phone ,\'AMAZON\' as marketplace, null as MARKETPLACE_ID, AM.ORDER_ID, LINE_ITEM_ID::varchar as suborder_id, EEF.INVOICE_ID, AM.ORDER_ID as reference_code, eef.manifest_date, null as SHIPPING_LAST_UPDATE_DATE, upper(EEF.SHIPPING_STATUS) SHIPPING_STATUS, AM.sku, null as sku_type, AM.product_id, product_name_final as product_name, null as currency, AM.is_refund, AM.city, AM.state, case when lower(am.order_status) like \'%cancel%\' then \'cancelled\' when lower(am.order_status) like \'%return%\' then \'returned\' else am.order_status end order_status, order_timestamp :: date as order_date, order_timestamp, AM.shipping_price, null as NUMBER_OF_PRODUCTS_IN_COMBO, quantity as suborder_quantity, null as SHIPPED_QUANTITY, null as RETURNED_QUANTITY, null as CANCELLED_QUANTITY, null as RETURN_SALES, null as CANCEL_SALES, AM.tax, null as SUBORDER_MRP, coalesce(s.Product_Category,AM.category) as category, product_name_final as Product_Name_Mapped, null as Report_Category, null as Product_Pack, null as Product_quantity, null as DISCOUNT, total_sales as selling_price, null as MRP_SALES, null as DISCOUNT_MRP, null as NEW_CUSTOMER_FLAG, null as NEW_CUSTOMER_FLAG_MONTH, WAREHOUSE_NAME, AM.is_refund as return_flag, null as DAYS_IN_SHIPMENT, \'AMAZON\' as CHANNEL, NULL as Shopify_UTM_source, NULL as Shopify_UTM_Medium, NULL AS UTM_CAMPAIGN, eef.payment_mode, null as IMPORT_DATE, null as LAST_UPDATE_DATE, EEF.INVOICE_DATE, null as company_name, null as pin_code, NULL as FINAL_UTM_CAMPAIGN, \'Synced\' as EasyEcom_Sync_Flag, null as cancelled_at, \'AMAZON\' as Data_Source from (SELECT * FROM RPSG_DB.MAPLEMONK.RPSG_DB_amazon_fact_items where lower(product_name) like \'%three60%\') AM LEFT JOIN rpsg_DB.maplemonk.fact_items_easyecom_drv eeF ON AM.ORDER_ID = EEF.reference_code AND LOWER(AM.SKU) = LOWER(EEF.SKU) left join (select * from (select sku, brand , category as Product_Category, \"Product Name\" AS PRODUCT_NAME1, \"Pack Size\"AS Product_Pack, null as Report_Category, null as product_quantity, row_number() over (partition by lower(sku) order by 1) rw from rpsg_DB.maplemonk.three60_sku_master where sku is not null and lower(marketplace) like \'%amazon%\') where rw=1 ) S on lower(Am.sku)=lower(s.sku) ; create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 as With CTE as ( select CUSTOMER_ID, appointment_id, start_date, phone, LAG(start_date, 1) OVER (partition by CUSTOMER_ID ORDER BY start_date desc) AS next_appointment_date, case when datediff(\'day\',start_date,next_appointment_date) >= 1 and datediff(\'day\',start_date,next_appointment_date) < 90 then dateadd(\'day\',-1,next_appointment_date) else dateadd(\'day\',90,start_date) end as end_date from ( SELECT DISTINCT ap.CUSTOMER_ID, ap.start_date, cs.phone, ap.id as appointment_id FROM (SELECT id, customer_id, end_timestamp::date AS start_date, ROW_NUMBER() OVER(PARTITION BY customer_id, end_timestamp::date ORDER BY 1) AS rw FROM rpsg_db.maplemonk.pg_three60you_appointment WHERE LOWER(status) = \'completed\') ap LEFT JOIN rpsg_db.maplemonk.pg_three60you_customer cs ON ap.customer_id = cs.id WHERE ap.rw = 1 ) ) select s.*,appointment_id, case when appointment_id IS not null then row_number() over(partition by appointment_id order by order_date asc) END AS rw, case when rw=1 AND lower(temp_shop_name) = \'three60plus\' then \'CONSULTATIONS\' else coalesce(temp_shop_name,\'CONSULTATIONS\') END AS SHOP_NAME from RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 s left join cte c on right(regexp_replace(c.phone, \'[^a-zA-Z0-9]+\'),10) = right(regexp_replace(s.phone, \'[^a-zA-Z0-9]+\'),10) and s.order_date::date >= c.start_date and s.order_date::date <= end_date ; create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 as select *, upper(b.\"Mapped Status\") Final_Status from RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 a left join (select distinct status, \"Mapped Status\" from rpsg_db.maplemonk.shipment_status_mapping) b on lower(case when lower(a.order_status) in (\'cancelled\') then a.order_status else coalesce(a.shipping_status, a.order_status) end)= lower(b.status); create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 as WITH GA_MAPPING as ( select rf.*,final_channel from ( select a.*, transactionid1,case when lower(transactionid) like \'pt%\' then transactionid else transactionid1 end as final_transaction from rpsg_db.maplemonk.ga4_three60you_orders_by_source a left join ( select distinct id, replace(PARSE_JSON(data):transactionId,\'\"\',\'\')transactionid1 from rpsg_db.maplemonk.pg_three60you_payment )b on lower(a.transactionid) = lower(b.id) )rf left join ( select * from ( select *, row_number() over(partition by lower(ifnull(source,\'\')),lower(ifnull(medium,\'\')) order by 1)rw, from rpsg_db.maplemonk.three60you_ga_channel_mapping )where rw=1 and (source is not null and medium is not null) )ga_mapping on lower(ga_mapping.source) = lower(rf.source) and lower(ga_mapping.medium) = lower(rf.medium) ), Get_reference_code as ( select distinct obs.source,obs.medium,final_channel,o.display_id from ga_mapping obs left join (select * from rpsg_db.maplemonk.pg_three60you_payment_transaction where lower(status) = \'authorized\') pt on lower(pt.id) = lower(obs.final_transaction) left join rpsg_db.maplemonk.pg_three60you_order o on lower(pt.cart_id) = lower(o.cart_id) ) select rf.source, rf.medium, case when lower(marketplace) = \'amazon\' then \'AMAZON\' else coalesce(rf.final_channel,\'Mapping\') end as final_channel, SCID.* from RPSG_DB.maplemonk.sales_consolidated_intermediate_three60 SCID left join Get_reference_code rf on lower(scid.reference_code) = lower(rf.display_id) ; create or replace table rpsg_DB.maplemonk.Final_customerID_three60 as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_THREE60 ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_Three60 ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date::date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_THREE60 o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from rpsg_DB.maplemonk.Final_customerID_three60) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from rpsg_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre drop COLUMN new_customer_flag ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN new_customer_flag varchar(50); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre drop COLUMN new_customer_flag_month ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN acquisition_marketplace varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre drop COLUMN ACQUISITION_DATE ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN ACQUISITION_DATE timestamp; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ADD COLUMN SAME_DAY_ORDERNO number; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS A SET A.SAME_DAY_ORDERNO = B.rw FROM ( select distinct customer_id_final ,order_id ,rank() over (partition by customer_id_final, order_date order by order_date, order_id) as rw from rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre ) AS B Where A.order_id = B.order_id; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id_final , min(order_Date) OVER ( partition BY customer_id_final) ACQUISITION_DATE from rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre B where (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) ) AS B where A.customer_id_final = B.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = ACQUISITION_DATE and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) then \'New\' WHEN Order_Date < ACQUISITION_DATE or acquisition_date is null THEN \'Yet to make completed order\' WHEN Order_Date > ACQUISITION_DATE then \'Repeat\' END AS Flag FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre )AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) in (\'cancelled\',\'returned\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date::date, \'month\') = Last_day(acquisition_date::date, \'month\') THEN \'New\' WHEN Last_day(order_date::date, \'month\') < Last_day(acquisition_date::date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date::date, \'month\') > Last_day(acquisition_date::date, \'month\') THEN \'Repeat\' END AS Flag FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE rpsg_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel , marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, FINAL_CHANNEL channel, marketplace, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre where (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end)) res WHERE order_date=firstorderdate; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS a SET a.acquisition_channel=b.channel FROM rpsg_db.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS a SET a.acquisition_marketplace=b.marketplace FROM rpsg_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE rpsg_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, category, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, category, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre where (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end))res WHERE order_date=firstorderdate; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_three60_pre AS A SET A.acquisition_product=B.category FROM ( SELECT * FROM rpsg_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final; create or replace table rpsg_DB.maplemonk.SALES_CONSOLIDATED_Three60 as select * from rpsg_DB.maplemonk.SALES_CONSOLIDATED_THREE60_PRE where (order_date::date >= \'2024-05-01\' or (order_date::date < \'2024-05-01\' and not(lower(ifnull(final_status,\'\')) like any (\'%cancel%\',\'%rto%\',\'%return%\')))) and ifnull(reference_code,\'\') not in (select distinct ifnull(reference_orders,\'\') from rpsg_db.maplemonk.three60you_test_orders); create or replace table RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_Three60 as select A.*,case when upper(s.brand) like \'%THREE60PLUS%\' then \'THREE60PLUS\' when upper(s.brand) like \'%THREE60%\' then \'THREE60\' else upper(marketplace) end as temp_SHOP_NAME ,upper(s.Product_Category) category ,upper(s.Product_name) Product_Name_Mapped ,upper(s.Report_Category) Report_Category ,upper(s.Product_Pack) Product_Pack ,\'THREE60\' as data_source ,s.product_quantity AS Product_quantity from (select ORDER_ID ,INVOICE_ID ,RI.VALUE:\"suborder_id\" SUBORDER_ID ,REFERENCE_CODE ,CREDIT_NOTE_ID ,CREDIT_NOTE_NUMBER ,try_to_timestamp(ORDER_DATE) ORDER_DATE ,try_to_timestamp(INVOICE_DATE) INVOICE_DATE ,try_to_timestamp(RETURN_DATE) RETURN_DATE ,try_to_timestamp(MANIFEST_DATE) MANIFEST_DATE ,try_to_timestamp(IMPORT_DATE) IMPORT_DATE ,try_to_timestamp(LAST_UPDATE_DATE) LAST_UPDATE_DATE ,RI.VALUE:company_product_id COMPANY_PRODUCT_ID ,replace(RI.VALUE:productName,\'\"\',\'\') PRODUCTNAME ,RI.VALUE:product_id PRODUCT_ID ,replace(RI.VALUE:sku,\'\"\',\'\') SKU ,MARKETPLACE ,COMPANY_NAME ,MARKETPLACE_ID ,REPLACEMENT_ORDER ,replace(RI.VALUE:return_reason,\'\"\',\'\') RETURN_REASON ,ifnull(RI.VALUE:returned_item_quantity::float,0) RETURNED_QUANTITY ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) RETURN_AMOUNT_WITHOUT_TAX ,ifnull(RI.Value:credit_note_total_item_tax::float,0) RETURN_TAX ,ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0) RETURN_SHIPPING_CHARGE ,ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) RETURN_MISC ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) + ifnull(RI.Value:credit_note_total_item_tax::float,0) + ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0)+ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) TOTAL_RETURN_AMOUNT ,row_number() over (partition by credit_note_number, order_id, suborder_id, invoice_id, company_product_id order by last_update_date desc) rw from ( select * from RPSG_DB.MAPLEMONK.easyecom_vl_returns where lower(marketplace) like any (\'%three60%\') and reference_code not in (select distinct reference_orders from rpsg_db.maplemonk.three60you_test_orders) )R, LATERAL flatten(INPUT => R.ITEMS) RI ) A left join (select * from (select sku, brand , category as Product_Category, \"Product Name\" AS PRODUCT_NAME, \"Pack Size\"AS Product_Pack, null as Report_Category, null as product_quantity, row_number() over (partition by lower(sku) order by 1) rw from rpsg_DB.maplemonk.three60_sku_master where sku is not null and lower(marketplace) like \'%three60%\') where rw=1 ) S on lower(A.sku)=lower(s.sku) where A.rw=1 union select A.*,case when upper(s.brand) like \'%THREE60PLUS%\' then \'THREE60PLUS\' when upper(s.brand) like \'%THREE60%\' then \'THREE60\' else upper(a.marketplace) end as temp_SHOP_NAME ,upper(s.Product_Category) category ,upper(s.Product_name) Product_Name_Mapped ,upper(s.Report_Category) Report_Category ,upper(s.Product_Pack) Product_Pack ,\'AMAZON\' as data_source ,s.product_quantity AS Product_quantity from (select ORDER_ID ,INVOICE_ID ,RI.VALUE:\"suborder_id\" SUBORDER_ID ,REFERENCE_CODE ,CREDIT_NOTE_ID ,CREDIT_NOTE_NUMBER ,try_to_timestamp(ORDER_DATE) ORDER_DATE ,try_to_timestamp(INVOICE_DATE) INVOICE_DATE ,try_to_timestamp(RETURN_DATE) RETURN_DATE ,try_to_timestamp(MANIFEST_DATE) MANIFEST_DATE ,try_to_timestamp(IMPORT_DATE) IMPORT_DATE ,try_to_timestamp(LAST_UPDATE_DATE) LAST_UPDATE_DATE ,RI.VALUE:company_product_id COMPANY_PRODUCT_ID ,replace(RI.VALUE:productName,\'\"\',\'\') PRODUCTNAME ,RI.VALUE:product_id PRODUCT_ID ,replace(RI.VALUE:sku,\'\"\',\'\') SKU ,case when lower(MARKETPLACE) like \'%amazon%\' then \'AMAZON\' when lower(marketplace) like \'%flipkart%\' then \'FLIPKART\' end as marketplace ,COMPANY_NAME ,MARKETPLACE_ID ,REPLACEMENT_ORDER ,replace(RI.VALUE:return_reason,\'\"\',\'\') RETURN_REASON ,ifnull(RI.VALUE:returned_item_quantity::float,0) RETURNED_QUANTITY ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) RETURN_AMOUNT_WITHOUT_TAX ,ifnull(RI.Value:credit_note_total_item_tax::float,0) RETURN_TAX ,ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0) RETURN_SHIPPING_CHARGE ,ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) RETURN_MISC ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) + ifnull(RI.Value:credit_note_total_item_tax::float,0) + ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0)+ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) TOTAL_RETURN_AMOUNT ,row_number() over (partition by credit_note_number, order_id, suborder_id, invoice_id, company_product_id order by last_update_date desc) rw from ( select * from RPSG_DB.MAPLEMONK.easyecom_vl_returns where lower(marketplace) like any (\'%amazon%\') and reference_code not in (select distinct reference_orders from rpsg_db.maplemonk.three60you_test_orders) )R, LATERAL flatten(INPUT => R.ITEMS) RI ) A left join (select * from (select sku, brand , category as Product_Category, \"Product Name\" AS PRODUCT_NAME, \"Pack Size\"AS Product_Pack, null as Report_Category, null as product_quantity, row_number() over (partition by lower(sku) order by 1) rw from rpsg_DB.maplemonk.three60_sku_master where sku is not null and lower(marketplace) like \'%amazon%\') where rw=1 ) S on lower(A.sku)=lower(s.sku) where A.rw=1 and brand is not null ; create or replace table RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_Three60 as select s.*, coalesce(shop_name1,temp_shop_name) SHOP_NAME from RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_Three60 s left join (select reference_code as reference_code1, suborder_id as suborder_id1, shop_name as shop_name1 from rpsg_DB.maplemonk.SALES_CONSOLIDATED_Three60) c on (c.reference_code1) = lower(s.reference_code) and lower(c.suborder_id1) = lower(s.suborder_id) ; Create or replace table RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_three60 as select ifnull(FE.channel,\'Mapping\') CHANNEL ,FE.Customer_id_final ,FE.phone ,FR.* from rpsg_db.maplemonk.fact_items_easyecom_returns_detailed_three60 FR left join (select * from (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, FINAL_CHANNEL channel, customer_id_final, phone, row_number() over (partition by replace(reference_code,\'#\',\'\') order by 1) rw from RPSG_DB.MAPLEMONK.sales_consolidated_three60 ) where rw=1) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table RPSG_DB.MAPLEMONK.EASYECOM_RETURNS_SUMMARY_three60 as select MARKETPLACE ,shop_name ,data_source ,COMPANY_NAME ,MARKETPLACE_ID ,Return_Date ,CHANNEL ,PRODUCT_NAME_MAPPED ,reference_code ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_three60 group by 1,2,3,4,5,6,7,8,9 order by 5 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        