{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.Maplemonk.redtape_db_sales_consolidated_intermediate as select Null as customer_id ,upper(b.SHOP_NAME) as SHOP_NAME ,upper(marketplace) as marketplace ,upper(marketplace) AS CHANNEL ,upper(marketplace) AS SOURCE ,b.ORDER_ID ,reference_code ,contact_num as PHONE ,customer_name as NAME ,email as EMAIL ,b.shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.easyecom_sku PRODUCT_ID ,upper(PRODUCTNAME) AS PRODUCT_NAME ,CURRENCY ,upper(CITY) City ,upper(STATE) AS State ,upper(ORDER_STATUS) as Order_Status ,ORDER_DATE::date AS Order_Date ,SUBORDER_QUANTITY AS QUANTITY ,ifnull(SELLING_PRICE,0)-ifnull(tax,0)+ifnull(DISCOUNT,0) AS GROSS_SALES_BEFORE_TAX ,case when SUBORDER_QUANTITY*SUBORDER_MRP - (SELLING_PRICE - SHIPPING_PRICE) < 0 then 0 else SUBORDER_QUANTITY*SUBORDER_MRP - (SELLING_PRICE -SHIPPING_PRICE) end AS DISCOUNT ,TAX ,SHIPPING_PRICE ,SELLING_PRICE AS SELLING_PRICE ,upper(ORDER_STATUS) as OMS_Order_Status ,upper(b.Shipping_status) AS SHIPPING_STATUS ,upper(case when lower(oms_order_status) like \'%cancel%\' or lower(b.Shipping_status) like \'%cancel%\' then \'CANCELLED\' else b.shipping_status end) FINAL_SHIPPING_STATUS ,Marketplace_LineItem_ID as SALEORDERITEMCODE ,suborder_id as SALES_ORDER_ITEM_ID ,b.awb AWB ,NULL Payment_Gateway ,b.payment_mode Payment_Mode ,UPPER(b.courier) COURIER ,b.MANIFEST_DATE as DISPATCH_DATE ,b.DELIVERED_DATE DELIVERED_DATE ,case when lower(b.shipping_status) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then suborder_quantity::int end returned_quantity ,case when RETURN_FLAG = 1 and lower(order_status) not in (\'cancelled\') then ifnull(is_refund,0) end returned_sales ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then suborder_quantity::int end cancelled_quantity ,new_customer_flag::varchar as NEW_CUSTOMER_FLAG ,NULL as ACQUISITION_PRODUCT ,Days_in_shipment AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,b.SKU as SKU_CODE ,upper(mapped_product_name) as PRODUCT_NAME_FINAL ,upper(mapped_category) as PRODUCT_CATEGORY ,b.brand ,b.size ,b.colour ,upper(WAREHOUSE_NAME) WAREHOUSE ,b.pincode ,b.shipping_status easyecom_status from redtape_db.Maplemonk.redtape_db_EasyEcom_FACT_ITEMS b ; create or replace table redtape_db.Maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from redtape_db.Maplemonk.redtape_db_sales_consolidated_intermediate ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from redtape_db.Maplemonk.redtape_db_sales_consolidated_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table redtape_db.Maplemonk.redtape_db_sales_consolidated as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from redtape_db.Maplemonk.redtape_db_sales_consolidated_intermediate o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from redtape_db.Maplemonk.Final_customerID ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from redtape_db.Maplemonk.Final_customerID where contact_num is null )d on d.email = m.email; ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated drop COLUMN new_customer_flag ; ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated ADD COLUMN new_customer_flag varchar(50); ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated drop COLUMN acquisition_product ; ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE redtape_db.Maplemonk.redtape_db_sales_consolidated ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM redtape_db.Maplemonk.redtape_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM redtape_db.Maplemonk.redtape_db_sales_consolidated)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE redtape_db.Maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM redtape_db.Maplemonk.redtape_db_sales_consolidated ) res WHERE order_date=firstorderdate; UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated AS a SET a.acquisition_channel=b.channel FROM redtape_db.Maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated AS a SET a.acquisition_marketplace=b.marketplace FROM redtape_db.Maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE redtape_db.Maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM redtape_db.Maplemonk.redtape_db_sales_consolidated )res WHERE order_date=firstorderdate; UPDATE redtape_db.Maplemonk.redtape_db_sales_consolidated AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM redtape_db.Maplemonk.temp_product_1 WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; Create or replace table redtape_db.Maplemonk.redtape_db_easyecom_returns_fact_items as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from redtape_db.Maplemonk.redtape_db_easyecom_returns_intermediate FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from redtape_db.Maplemonk.redtape_db_sales_consolidated) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table redtape_db.Maplemonk.redtape_db_RETURNS_CONSOLIDATED as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from redtape_db.Maplemonk.redtape_db_easyecom_returns_fact_items group by 1,2,3 order by 2 desc;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            