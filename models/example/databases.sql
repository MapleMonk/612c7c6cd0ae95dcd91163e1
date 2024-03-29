{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE or replace TABLE ras_db.MAPLEMONK.shipment_status_mapping (SHIPPING_STATUS VARCHAR(16777216),Mapped_Status VARCHAR(16777216)); Insert into ras_db.MAPLEMONK.shipment_status_mapping values (\'DELIVERED\',\'Delivered\') ,(\'CANCELLED\',\'Cancelled\') ,(\'DELIVERED TO ORIGIN\',\'Returned\') ,(\'SHIPMENT CREATED\',\'Shipment Created\') ,(\'RETURNED\',\'Returned\') ,(\'IN TRANSIT\',\'In Transit\') ,(\'DELIVERY DELAYED\',\'In Transit\') ,(\'DELIVERED TO ORIGIN\',\'RTO\') ,(\'CONFIRMED\',\'Open\') ,(\'IN_TRANSIT\',\'In Transit\') ,(\'RTO DELIVERED\',\'RTO\') ,(\'CANCELED\',\'Cancelled\') ,(\'RTO IN TRANSIT\',\'RTO\') ,(\'REACHED AT DESTINATION HUB\',\'In Transit\') ,(\'PICKED UP\',\'In Transit\') ,(\'UNDELIVERED-2ND ATTEMPT\',\'In Transit\') ,(\'RTO_OFD\',\'RTO\') ,(\'OUT FOR DELIVERY\',\'Out for delivery\') ,(\'RTO INITIATED\',\'RTO\') ,(\'PICKUP EXCEPTION\',\'Pickup Error\') ,(\'CANCELLATION REQUESTED\',\'Pickup Error\') ,(\'READY TO SHIP\',\'Ready to Ship\') ,(\'OUT_FOR_DELIVERY\',\'Out for delivery\') ,(\'SHIPPED\',\'In Transit\') ,(\'PICKUP RESCHEDULED\',\'Pickup Error\') ,(\'UNDELIVERED-3RD ATTEMPT\',\'In Transit\') ,(\'MISROUTED\',\'Misrouted\') ,(\'PICKUP SCHEDULED\',\'Pickup Error\') ,(\'UNDELIVERED-1ST ATTEMPT\',\'In Transit\') ,(\'Not Traceble\',\'Pickup Error\') ,(\'UNDELIVERED\',\'In Transit\') ,(\'LOST\',\'Lost\') ,(\'IN TRANSIT-EN-ROUTE\',\'In Transit\') ,(\'Indian Speed Post\',\'Indian Speed Post\') ,(\'Exception\',\'Exception\') ,(\'Shipment Booked\',\'Pickup Error\') ,(\'Assigned\',\'Assigned\') ,(\'Pending\',\'Pending\') ,(\'Printed\',\'Printed\') ,(\'IN TRANSIT-AT DESTINATION HUB\',\'In Transit\') ,(\'Upcoming\',\'Order Yet To sync\') ,(\'RTO-In Transit\',\'RTO\') ,(\'PICKUP ERROR\',\'Pickup Error\') ,(\'RTO-Out for Delivery\',\'RTO\') ,(\'RTO-Delivered\',\'RTO\') ,(\'OUT FOR PICKUP\',\'Pickup Error\') ,(\'RTO_NDR\',\'RTO\') ,(\'RTO-Exception\',\'Exception\') ,(\'Delayed\',\'In Transit\') ,(\'IN TRANSIT-AT SOURCE HUB\',\'In Transit\') ,(\'RTS\',\'Underprocess\') ,(\'Manifested\',\'Pickup Error\') ,(\'Dispatched\',\'In Transit\') ,(\'RTO\',\'RTO\') ,(\'Not Picked\',\'Pickup Error\') ,(\'DAMAGED\',\'Damaged\') ,(\'Closed\',\'Cancelled\') ,(\'Open\',\'Open\') ,(\'On Hold\',\'On Hold\') ,(\'Shipped - Returned to Seller\',\'RTO\') ,(\'Shipped - Delivered to Buyer\',\'Delivered\') ,(\'Shipped - Picked Up\',\'In Transit\') ,(\'Shipped - Rejected by Buyer\',\'RTO\') ,(\'Shopify_Processed\',\'Shopify_Processed\') ,(\'Ready to dispatch\',\'Ready to Dispatch\') ,(\'RTO Undelivered\',\'RTO\') ,(\'Shipment Lost\',\'Lost\') ,(\'Shipment Error\',\'Shipment Error\') ,(\'RTO In-Transit\',\'RTO\') ,(\'Dispatch / RTO\',\'RTO\') ,(\'Pending Pick-up\',\'Pending\') ,(\'Dispatch/Intransit\',\'In Transit\') ,(\'Dispatch /Lost and Damange\',\'Lost\') ,(\'Dispatch /Undelivered\',\'Undelivered\') ,(\'Non serviceable\',\'Non Serviceable\') ,(\'Not serviceable\',\'Non Serviceable\') ,(\'Dispatch/Delivered\',\'Delivered\') ,(\'Refund\',\'Refund\') ,(\'F&P\',\'F&P\') ,(\'SHIPPED - RETURNING TO SELLER\',\'RTO\') ,(\'SHIPPED - LOST IN TRANSIT\',\'Lost\') ,(\'SHIPPED - DAMAGED\',\'Damaged\') ,(\'SHIPPED - OUT FOR DELIVERY\',\'Out for delivery\'); create or replace table ras_db.MAPLEMONK.ras_db_sales_consolidated_intermediate_moody as select b.customer_id ,upper(b.SHOP_NAME) SHOP_NAME ,upper(b.shop_name) as marketplace ,Upper(b.FINAL_UTM_CHANNEL) AS CHANNEL ,Upper(b.FINAL_UTM_SOURCE) AS SOURCE ,b.ORDER_ID ,order_name reference_code ,b.PHONE ,b.NAME ,b.EMAIL ,coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime) AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,Upper(b.PRODUCT_NAME) PRODUCT_NAME ,b.CURRENCY ,Upper(b.CITY) As CITY ,Upper(b.STATE) AS State ,Upper(b.ORDER_STATUS) ORDER_STATUS ,b.ORDER_TIMESTAMP::date AS Order_Date ,b.QUANTITY ,b.GROSS_SALES_BEFORE_TAX AS GROSS_SALES_BEFORE_TAX ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT ,b.TAX ,b.SHIPPING_PRICE ,b.TOTAL_SALES AS SELLING_PRICE ,UPPER(coalesce(c.order_status,d.order_status)) as OMS_order_status ,UPPER(coalesce(b.shipping_status, c.shipping_status,d.shipping_status)) AS SHIPPING_STATUS ,upper(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) FINAL_SHIPPING_STATUS ,b.LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,d.sales_order_item_id as SALES_ORDER_ITEM_ID ,coalesce(b.awb,c.awb,d.awb) AWB ,UPPER(b.GATEWAY) PAYMENT_GATEWAY ,upper(coalesce(c.payment_mode,d.payment_mode)) Payment_Mode ,Upper(coalesce(c.Courier,d.courier,b.courier)) AS COURIER ,coalesce(b.Shipping_created_at,c.manifest_date,d.dispatch_date) AS DISPATCH_DATE ,coalesce(c.delivered_date,d.delivered_date,case when b.shipping_status like \'delivered\' then b.shipping_status_update_date end) AS DELIVERED_DATE ,case when lower(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) = \'delivered\' then 1 else 0 end AS DELIVERED_STATUS ,coalesce(case when b.IS_REFUND=1 and lower(b.order_status) not in (\'cancelled\') then 1 end,c.IS_REFUND, d.return_flag) AS RETURN_FLAG ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_quantity,0) end returned_quantity ,case when RETURN_FLAG = 1 and lower(b.order_status) not in (\'cancelled\') then ifnull(refund_value,0) end returned_sales ,case when lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,b.shopify_new_customer_flag as NEW_CUSTOMER_FLAG ,Upper(b.shopify_acquisition_product) as acquisition_product ,case when lower(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) in (\'delivered\',\'delivered to origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(coalesce(b.shipping_status_update_date,c.shipping_last_update_date::datetime, d.shipping_last_update_date::datetime))) when lower(coalesce(shipmap.final_shipping_status,b.shipping_status, c.shipping_status,d.shipping_status)) in (\'in transit\', \'shipment created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int as Days_in_Shipment ,b.shopify_acquisition_date AS ACQUSITION_DATE ,b.SKU_CODE ,UPPER(b.PRODUCT_NAME_FINAL) PRODUCT_NAME_FINAL ,UPPER(b.PRODUCT_CATEGORY) PRODUCT_CATEGORY ,upper(b.PRODUCT_SUB_CATEGORY) PRODUCT_SUB_CATEGORY ,upper(c.warehouse_name) WAREHOUSE ,\'Moody\' as brand from ras_db.MAPLEMONK.ras_db_SHOPIFY_FACT_ITEMS_moody b left join (select * from ( select * ,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from ras_db.MAPLEMONK.ras_db_EasyEcom_FACT_ITEMS ) z where z.rw = 1 and lower(marketplace) in (\'shopifymarketplace\') ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.LINE_ITEM_ID=c.Marketplace_LineItem_ID and lower(b.shop_name) = case when lower(c.marketplace) = \'shopifymarketplace\' then \'shopify_moody\' end left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,PAYMENT_MODE ,email ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from ras_db.MAPLEMONK.ras_db_UNICOMMERCE_FACT_ITEMS where lower(marketplace) like any (\'%shopify%\')) where rw=1 ) d on b.order_id=d.order_id and b.line_item_id=split_part(d.saleorderitemcode,\'-\',0) left join ( select * from ( select upper(Shipping_status) shipping_status ,upper(mapped_status) final_shipping_status ,row_number() over (partition by lower(shipping_Status) order by 1) rw from ras_db.MAPLEMONK.shipment_status_mapping ) where rw = 1 ) ShipMap on lower(coalesce(b.shipping_status,c.shipping_status,d.shipping_status,b.ORDER_STATUS)) = lower(ShipMap.shipping_status) ; create or replace table ras_db.MAPLEMONK.Final_customerID_moody as with new_phone_numbers as ( select phone, contact_num, 19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from ras_db.MAPLEMONK.ras_db_sales_consolidated_intermediate_moody ) a ), int as ( select contact_num, email, coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email, maple_monk_id, 19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email, maple_monk_id from ( select phone as contact_num, email from ras_db.MAPLEMONK.ras_db_sales_consolidated_intermediate_moody ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table ras_db.MAPLEMONK.ras_db_sales_consolidated_moody as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from ( select c.maple_monk_id as maple_monk_id_phone, o.* from ras_db.MAPLEMONK.ras_db_sales_consolidated_intermediate_moody o left join ( select * from ( select contact_num phone, maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from ras_db.MAPLEMONK.Final_customerID_moody ) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10) )m left join ( select distinct maple_monk_id, email from ras_db.MAPLEMONK.Final_customerID_moody where contact_num is null )d on d.email = m.email; ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody drop COLUMN new_customer_flag ; ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody ADD COLUMN new_customer_flag varchar(50); ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody drop COLUMN acquisition_product ; ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM ras_db.MAPLEMONK.ras_db_sales_consolidated_moody) AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM ras_db.MAPLEMONK.ras_db_sales_consolidated_moody) AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE ras_db.MAPLEMONK.temp_source_1_moody AS SELECT DISTINCT customer_id_final, channel, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source as channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM ras_db.MAPLEMONK.ras_db_sales_consolidated_moody ) res WHERE order_date=firstorderdate; UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody AS a SET a.acquisition_channel=b.channel FROM ras_db.MAPLEMONK.temp_source_1_moody b WHERE a.customer_id_final = b.customer_id_final; UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody AS a SET a.acquisition_marketplace=b.marketplace FROM ras_db.MAPLEMONK.temp_source_1_moody b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE ras_db.MAPLEMONK.temp_product_1_moody AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM ras_db.MAPLEMONK.ras_db_sales_consolidated_moody )res WHERE order_date=firstorderdate; UPDATE ras_db.MAPLEMONK.ras_db_sales_consolidated_moody AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM ras_db.MAPLEMONK.temp_product_1_moody WHERE rowid=1 )B WHERE A.customer_id_final = B.customer_id_final; CREATE table if not exists ras_db.MAPLEMONK.ras_db_easyecom_returns_intermediate_moody (ORDER_ID NUMBER,INVOICE_ID NUMBER,SUBORDER_ID VARIANT,REFERENCE_CODE VARCHAR,CREDIT_NOTE_ID NUMBER,ORDER_DATE TIMESTAMP_NTZ,INVOICE_DATE TIMESTAMP_NTZ,RETURN_DATE TIMESTAMP_NTZ,MANIFEST_DATE TIMESTAMP_NTZ,IMPORT_DATE TIMESTAMP_NTZ,LAST_UPDATE_DATE TIMESTAMP_NTZ,COMPANY_PRODUCT_ID VARIANT,PRODUCTNAME VARCHAR,PRODUCT_ID VARIANT,SKU VARCHAR,MARKETPLACE VARCHAR,MARKETPLACE_ID NUMBER,REPLACEMENT_ORDER NUMBER,RETURN_REASON VARCHAR,RETURNED_QUANTITY FLOAT,RETURN_AMOUNT_WITHOUT_TAX FLOAT,RETURN_TAX FLOAT,RETURN_SHIPPING_CHARGE FLOAT,RETURN_MISC FLOAT,TOTAL_RETURN_AMOUNT FLOAT) ; Create or replace table ras_db.MAPLEMONK.ras_db_easyecom_returns_fact_items_moody as select ifnull(FE.Source,\'NA\') Marketing_CHANNEL ,FR.* from ras_db.MAPLEMONK.ras_db_easyecom_returns_intermediate_moody FR left join (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, Source from ras_db.MAPLEMONK.ras_db_sales_consolidated_moody) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table ras_db.MAPLEMONK.ras_db_RETURNS_CONSOLIDATED_moody as select upper(MARKETPLACE) Marketplace ,Return_Date ,upper(Marketing_CHANNEL) Marketing_channel ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from ras_db.MAPLEMONK.ras_db_easyecom_returns_fact_items_moody group by 1,2,3 order by 2 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ras_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        