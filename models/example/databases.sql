{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX as with orders as (select CUSTOMER_ID ,SHOP_NAME ,FINAL_UTM_SOURCE AS SOURCE ,FINAL_UTM_CHANNEL AS CHANNEL ,b.ORDER_ID ,b.PHONE ,b.NAME ,b.EMAIL ,c.shipping_last_update_date AS SHIPPING_LAST_UPDATE_DATE ,b.SKU ,b.PRODUCT_ID ,b.PRODUCT_NAME ,b.CURRENCY ,coalesce(c.city, b.CITY) as CITY ,coalesce(c.STATE, b.state) AS State ,coalesce(c.ORDER_STATUS, b.order_status) as Order_status ,b.ORDER_TIMESTAMP AS Order_Date ,b.SHIPPING_PRICE ,b.QUANTITY ,b.DISCOUNT_BEFORE_TAX AS DISCOUNT_PLATFORM ,b.TAX ,b.TOTAL_SALES AS SELLING_PRICE ,b.is_refund ,b.order_status as Shopify_order_status ,c.order_status as unicommerce_order_status ,c.shipping_status AS unicommerce_SHIPPING_STATUS ,b.tags ,c.return_flag as unicommerce_return_flag ,c.shippingpackagecode AS SHIPPINGPACKAGECODE ,c.shippingpackagestatus AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,c.sales_order_item_id as SALES_ORDER_ITEM_ID ,c.awb ,c.payment_method ,c.Courier AS COURIER ,c.dispatch_date AS DISPATCH_DATE ,c.delivered_date AS DELIVERED_DATE ,coalesce(b.IS_REFUND,c.Return_flag) AS RETURN_FLAG ,case when coalesce(b.IS_REFUND,c.Return_flag) = 1 then b.quantity::int end returned_quantity ,case when coalesce(b.IS_REFUND,c.Return_flag) = 0 and lower(b.order_status) in (\'cancelled\') then b.quantity::int end cancelled_quantity ,b.SHOPIFY_NEW_CUSTOMER_FLAG as NEW_CUSTOMER_FLAG ,b.SHOPIFY_ACQUISITION_PRODUCT as ACQUISITION_PRODUCT ,c.days_in_shipment AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,b.SKU_CODE ,b.PRODUCT_NAME_FINAL ,b.PRODUCT_CATEGORY ,b.PRODUCT_SUPER_CATEGORY ,b.shopify_product_collection ,b.order_name ,b.net_sales_before_Tax ,b.GROSS_SALES_BEFORE_TAX ,b.refund_details from XYXX_db.maplemonk.FACT_ITEMS_XYXX b left join (select * from (select order_id ,city ,state ,saleorderitemcode ,sales_order_item_id ,shippingpackagecode ,SHIPPINGPACKAGESTATUS ,shipping_status ,order_status ,Courier ,Dispatch_Date ,Delivered_date ,Return_flag ,Return_quantity ,cancelled_quantity ,shipping_last_update_date ,days_in_shipment ,awb ,payment_method ,row_number() over (partition by order_id, split_part(saleorderitemcode,\'-\',0) order by shipping_last_update_date desc) rw from XYXX_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_XYXX_FINAL where lower(marketplace) like any (\'%shopify%\',\'%amazon%\')) where rw=1 )c on b.order_id=c.order_id and b.line_item_id=split_part(c.saleorderitemcode,\'-\',0) union all select CUSTOMER_ID ,MARKETPLACE ,SOURCE ,MARKETPLACE AS CHANNEL ,ORDER_ID ,PHONE ,NAME ,EMAIL ,SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE ,ORDER_STATUS ,ORDER_DATE Order_Date ,SHIPPING_PRICE ,SUBORDER_QUANTITY ,DISCOUNT DISCOUNT_PLATFORM ,TAX ,SELLING_PRICE ,NULL is_refund ,NULL as shopify_order_status ,order_status as unicommerce_order_status ,shipping_status AS unicommerce_SHIPPING_STATUS ,NULL tags ,return_flag ,SHIPPINGPACKAGECODE ,SHIPPINGPACKAGESTATUS ,SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID ,AWB ,PAYMENT_METHOD ,COURIER ,DISPATCH_DATE ,DELIVERED_DATE ,RETURN_FLAG ,RETURN_QUANTITY ,CANCELLED_QUANTITY ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,DAYS_IN_SHIPMENT ,ACQUISITION_DATE ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,PRODUCT_SUPER_CATEGORY ,NULL as shopify_product_collection ,marketplace_order_id as order_name ,ifnull(selling_price,0) - ifnull(tax,0) + ifnull(DISCOUNT,0) NET_SALES_BEFORE_TAX ,ifnull(selling_price,0) - ifnull(tax,0) - ifnull(shipping_price,0) + ifnull(DISCOUNT,0) GROSS_SALES_BEFORE_TAX ,null as refund_details from XYXX_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_XYXX_FINAL b where lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%shopify%\') and lower(marketplace) not like (\'%cred%\') ) select CUSTOMER_ID ,SHOP_NAME ,SOURCE ,CHANNEL ,ORDER_ID ,PHONE ,NAME ,EMAIL ,SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,b.PRODUCT_NAME ,CURRENCY ,CITY ,State ,Order_status ,Order_Date ,SHIPPING_PRICE ,QUANTITY ,DISCOUNT_PLATFORM ,TAX ,SELLING_PRICE ,SHIPPINGPACKAGECODE ,SHIPPINGPACKAGESTATUS ,SALEORDERITEMCODE ,SALES_ORDER_ITEM_ID ,b.awb ,payment_method ,coalesce(e.courier, b.Courier) COURIER ,b.unicommerce_SHIPPING_STATUS ,DISPATCH_DATE ,DELIVERED_DATE ,RETURN_FLAG ,returned_quantity ,cancelled_quantity ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,DAYS_IN_SHIPMENT ,ACQUSITION_DATE ,SKU_CODE ,b.PRODUCT_NAME_FINAL ,b.PRODUCT_CATEGORY ,b.PRODUCT_SUPER_CATEGORY ,b.shopify_product_collection ,order_name ,d.mrp*quantity mrp ,d.cogs*quantity cogs ,GROSS_SALES_BEFORE_TAX ,net_sales_before_tax ,ifnull(mrp,0) - ifnull(b.net_Sales_before_tax,0) discount ,f.\"Final Status\" final_status ,e.status shipment_partner_Status ,e.shipment_aggregator ,e.updated_date shipment_partner_updated_date ,refund_details ,div0(case when lower(f.\"Final Status\") in (\'canceled\',\'pending to dispatch\',\'pending to process\') then 0 else coalesce(e.shipping_charges, g.shipping_charges) end,count(1) over (partition by b.order_id)) Logistics ,div0(case when lower(f.\"Final Status\") = \'rto\' then Logistics else 0 end,count(1) over (partition by b.order_id)) as Return_Charges ,case when lower(f.\"Final Status\") = \'canceled\' then 0 else (case when lower(d.product_sub_Category) in (\'trunk\',\'brief\') then 2.4 else 5.3 end + case when lower(d.product_sub_Category) in (\'trunk\',\'brief\') then div0(17.35, count(1) over (partition by b.order_id)) else div0(29.8, count(1) over (partition by b.order_id)) end) end as packaging_cost ,div0(coalesce(e.cod_charges,g.cod_charges),count(1) over (partition by b.order_id)) Cash_Collection_Charges ,case when lower(shopify_order_status) in (\'cancelled\') then \'CANCELED\' when coalesce(is_refund,unicommerce_return_flag)=1 and lower(tags) like (\'%return%\') then upper(\'CUSTOMER REQUESTED RETURN\') when coalesce(is_refund,unicommerce_return_flag) = 1 and lower(tags) not like (\'%return%\') and ( lower(unicommerce_SHIPPING_STATUS) like \'%rto%\' or lower(shipment_partner_Status) like \'%rto%\') then \'RTO\' else COALESCE(shipment_partner_Status, unicommerce_SHIPPING_STATUS) end as final_shipping_status from orders b left join ( select * from (select sku_id, start_date, end_date, product_sub_category, product_category, product_name, try_cast(mrp as float) mrp, try_cast(cogs as float) cogs, row_number() over (partition by sku_id, start_date, end_date order by mrp desc) rw from xyxx_db.maplemonk.mapping_product_mrp_cogs) where rw=1 ) d on b.order_date::date >= start_date::date and b.order_date::date <=end_date::Date and b.sku_code = d.sku_id left join (select * from (select shipment_aggregator,awb,status,Shipping_Charges, courier, COD_Charges,updated_date, row_number() over (partition by awb order by updated_date desc) rw from xyxx_db.maplemonk.logistics_fact_items_xyxx) where rw=1 ) e on b.awb = e.awb left join (select * from (select * ,row_number() over (partition by lower(all_statuses) order by \"Final Status\") rw from xyxx_db.maplemonk.googlesheet_status_mapping) where rw=1) f on lower(coalesce(e.status, b.unicommerce_SHIPPING_STATUS, b.order_Status)) = lower(f.all_statuses) left join xyxx_db.maplemonk.mapping_shipment_cost g on g.from_date::date <= b.order_date::date and g.to_date::date >=b.order_date::date ; create or replace table XYXX_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(replace(phone,\' \',\'\'), \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table XYXX_DB.maplemonk.sales_consolidated_XYXX as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final , min(order_date) over(partition by customer_id_final) as acquisition_date , min(case when lower(order_status) not in (\'cancelled\') then order_date end) over(partition by customer_id_final) as first_complete_order_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from XYXX_DB.maplemonk.sales_consolidated_intermediate_XYXX o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from XYXX_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from XYXX_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX drop COLUMN new_customer_flag ; ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN new_customer_flag varchar(50); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX drop COLUMN acquisition_product ; ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_source varchar(16777216); ALTER TABLE XYXX_DB.maplemonk.sales_consolidated_XYXX ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE xyxx_db.maplemonk.sales_consolidated_xyxx AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = first_complete_order_date then \'New\' WHEN Order_Date < first_complete_order_date or first_complete_order_date is null THEN \'Yet to make completed order\' WHEN Order_Date > first_complete_order_date then \'Repeat\' END AS Flag FROM xyxx_db.maplemonk.sales_consolidated_xyxx)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE xyxx_db.maplemonk.sales_consolidated_xyxx SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE xyxx_db.maplemonk.sales_consolidated_xyxx AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(first_complete_order_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(first_complete_order_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(first_complete_order_date, \'month\') THEN \'Repeat\' END AS Flag FROM xyxx_db.maplemonk.sales_consolidated_xyxx)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE xyxx_db.maplemonk.sales_consolidated_xyxx SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE XYXX_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel , source, marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, source, channel, shop_name as marketplace, Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER ( partition BY customer_id_final) firstOrderdate FROM XYXX_DB.maplemonk.sales_consolidated_XYXX ) res WHERE order_date=firstorderdate; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS a SET a.acquisition_channel=b.channel FROM XYXX_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS a SET a.acquisition_source=b.source FROM XYXX_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS a SET a.acquisition_marketplace=b.marketplace FROM XYXX_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE XYXX_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, product_name_final, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, product_name_final, SELLING_PRICE , Min(case when lower(order_status) <> \'cancelled\' then order_date end) OVER (partition BY customer_id_final) firstOrderdate FROM XYXX_DB.maplemonk.sales_consolidated_XYXX )res WHERE order_date=firstorderdate; UPDATE XYXX_DB.maplemonk.sales_consolidated_XYXX AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM XYXX_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        