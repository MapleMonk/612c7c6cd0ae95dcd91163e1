{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_drv as select b.SHOP_NAME, coalesce(c.carrier_id ,NULL) as carrier_id, coalesce(c.courier ,NULL) as courier, coalesce(b.name ,c.Customer_Name) as Customer_Name, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, b.SHOP_NAME as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, line_item_id::varchar as Invoice_ID, order_name as Reference_Code, coalesce(c.manifest_date::datetime,NULL) as manifest_date, coalesce(c.shipping_last_update_date::datetime ,NULL) as shipping_last_update_date, coalesce(d.status, c.shipping_status ,NULL) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, b.PRODUCT_NAME as PRODUCTNAME, b.CURRENCY, b.IS_REFUND as RETURN_FLAG, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, coalesce(c.order_status,b.order_status) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, case when b.is_refund = 1 then total_sales end as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, b.TAX::float Tax, max(c.suborder_mrp) over (partition by b.ORDER_TIMESTAMP::date, b.sku) as suborder_mrp, b.category, b.discount_before_tax::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, coalesce(c.suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<(selling_price-b.TAX::float)) then b.discount else mrp_sales-selling_price-b.TAX::float end Discount_MRP, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, coalesce(c.warehouse_name,\'NA\') as Warehouse_Name, case when shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) when shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) end::int as Days_in_Shipment, FINAL_UTM_CHANNEL as Channel, c.payment_mode, c.import_date::datetime as import_date, c.last_update_date, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as EasyEcom_Sync_Flag from rpsg_db.maplemonk.FACT_ITEMS_SHOPIFY_DRV b left join (select * from ( select *,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from rpsg_db.maplemonk.fact_items_easyecom_drv ) z where z.rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.order_timestamp = c.order_date left join (select * from (select distinct \"Order Id\", status, row_number() over (partition by \"Order Id\" order by status) rw from temp_shipping_status) p where p.rw=1 ) d on replace(replace(b.order_name,\'#0000000\',\'\'),\'#\',\'\')=d.\"Order Id\" union all select b.SHOP_NAME, coalesce(c.carrier_id ,NULL) as carrier_id, coalesce(c.courier ,NULL) as courier, coalesce(b.name ,c.Customer_Name) as Customer_Name, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, b.SHOP_NAME as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, line_item_id::varchar as Invoice_ID, order_name as Reference_Code, coalesce(c.manifest_date::datetime,NULL) as manifest_date, coalesce(c.shipping_last_update_date::datetime ,NULL) as shipping_last_update_date, coalesce(d.status, c.shipping_status ,NULL) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, b.PRODUCT_NAME as PRODUCTNAME, b.CURRENCY, b.IS_REFUND as RETURN_FLAG, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, coalesce(c.order_status,b.order_status) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, case when b.is_refund = 1 then line_item_sales end as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then line_item_sales end as cancel_sales, b.TAX::float Tax, max(c.suborder_mrp) over (partition by b.ORDER_TIMESTAMP::date, b.sku) as suborder_mrp, b.category, b.discount_before_tax::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, coalesce(c.suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<(selling_price-b.TAX::float)) then b.discount else mrp_sales-selling_price-b.TAX::float end Discount_MRP, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, coalesce(c.warehouse_name,\'NA\') as Warehouse_Name, case when shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) when shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) end::int as Days_in_Shipment, FINAL_UTM_CHANNEL as Channel, c.payment_mode, c.import_date::datetime as import_date, c.last_update_date, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as EasyEcom_Sync_Flag from rpsg_db.maplemonk.FACT_ITEMS_SHOPIFY_HERBOBUILD b left join (select * from ( select *,row_number()over(partition by reference_code,order_date order by last_update_date desc) rw from rpsg_db.maplemonk.fact_items_easyecom_drv ) z where z.rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.order_timestamp = c.order_date left join (select * from (select distinct \"Order Id\", status, row_number() over (partition by \"Order Id\" order by status) rw from temp_shipping_status) p where p.rw=1 ) d on replace(replace(b.order_name,\'#0000000\',\'\'),\'#\',\'\')=d.\"Order Id\" union all select b.SHOP_NAME, coalesce(c.carrier_id ,NULL) as carrier_id, coalesce(c.courier ,NULL) as courier, coalesce(b.name ,c.Customer_Name) as Customer_Name, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, b.SHOP_NAME as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, line_item_id::varchar as Invoice_ID, order_name as Reference_Code, coalesce(c.manifest_date::datetime,NULL) as manifest_date, coalesce(c.shipping_last_update_date::datetime ,NULL) as shipping_last_update_date, coalesce(d.status, c.shipping_status ,NULL) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, b.PRODUCT_NAME as PRODUCTNAME, b.CURRENCY, b.IS_REFUND as RETURN_FLAG, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, coalesce(c.order_status,b.order_status) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, case when b.is_refund = 1 then total_sales end as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, b.TAX::float Tax, max(c.suborder_mrp) over (partition by b.ORDER_TIMESTAMP::date, b.sku) as suborder_mrp, b.category, b.discount_before_tax::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, coalesce(c.suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<(selling_price-b.TAX::float)) then b.discount else mrp_sales-selling_price-b.TAX::float end Discount_MRP, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, coalesce(c.warehouse_name,\'NA\') as Warehouse_Name, case when shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(b.ORDER_TIMESTAMP), getdate()) when shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) end::int as Days_in_Shipment, FINAL_UTM_CHANNEL as Channel, c.payment_mode, c.import_date::datetime as import_date, c.last_update_date, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as EasyEcom_Sync_Flag from rpsg_db.maplemonk.FACT_ITEMS_SHOPIFY_AYURVEDICSOURCE b left join (select * from ( select *,row_number()over(partition by reference_code,order_date order by last_update_date desc) rw from rpsg_db.maplemonk.fact_items_easyecom_drv ) z where z.rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.order_timestamp = c.order_date left join (select * from (select distinct \"Order Id\", status, row_number() over (partition by \"Order Id\" order by status) rw from temp_shipping_status) p where p.rw=1 ) d on replace(replace(b.order_name,\'#0000000\',\'\'),\'#\',\'\')=d.\"Order Id\" union all select SHOP_NAME ,CARRIER_ID ,COURIER ,CUSTOMER_NAME ,EMAIL ,CONTACT_NUM ,MARKETPLACE ,MARKETPLACE_ID ,ORDER_ID ,SUBORDER_ID ,INVOICE_ID ,REFERENCE_CODE ,MANIFEST_DATE ,SHIPPING_LAST_UPDATE_DATE ,coalesce(d.status,SHIPPING_STATUS) SHIPPING_STATUS ,SKU ,SKU_TYPE ,PRODUCT_ID ,PRODUCTNAME ,CURRENCY ,IS_REFUND ,CITY ,STATE ,ORDER_STATUS ,ORDER_DATE ,SHIPPING_PRICE ,NUMBER_OF_PRODUCTS_IN_COMBO ,SUBORDER_QUANTITY ,SHIPPED_QUANTITY ,RETURNED_QUANTITY ,CANCELLED_QUANTITY ,RETURN_SALES ,CANCEL_SALES ,TAX ,SUBORDER_MRP ,CATEGORY ,DISCOUNT ,SELLING_PRICE ,MRP_SALES ,DISCOUNT_MRP ,NEW_CUSTOMER_FLAG ,NEW_CUSTOMER_FLAG_MONTH ,WAREHOUSE_NAME ,DAYS_IN_SHIPMENT ,CHANNEL ,PAYMENT_MODE ,IMPORT_DATE ,LAST_UPDATE_DATE ,\'Synced\' as EasyEcom_Sync_Flag from rpsg_DB.maplemonk.fact_items_easyecom_drv b left join (select * from (select distinct \"Order Id\", status, row_number() over (partition by \"Order Id\" order by status) rw from temp_shipping_status) p where p.rw=1 ) d on replace(replace(b.reference_code,\'#0000000\',\'\'),\'#\',\'\')=d.\"Order Id\" where not(lower(b.marketplace) like \'%shopify%\'); create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_drv as select (case when GAC.CHANNEL is null then \'NA\' else GAC.channel end) as GA_Channel ,GAC.GA_SOURCE ,GAC.GA_MEDIUM ,GAC.VIEW_ID ,SCID.* from RPSG_DB.maplemonk.sales_consolidated_intermediate_drv SCID left join (select * from (select *, Row_number() OVER (partition BY ga_transactionid ORDER BY GA_DATE DESC) rw from ga_order_by_source_consolidated_drv) where rw=1) GAC on SCID.reference_code=GAC.GA_TRANSACTIONID; create or replace table rpsg_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_DRV ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_DRV ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_DRV o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from rpsg_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from rpsg_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV drop COLUMN new_customer_flag ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN new_customer_flag varchar(50); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV drop COLUMN new_customer_flag_month ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN acquisition_marketplace varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV drop COLUMN ACQUISITION_DATE ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN ACQUISITION_DATE timestamp; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ADD COLUMN SAME_DAY_ORDERNO number; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS A SET A.SAME_DAY_ORDERNO = B.rw FROM ( select distinct customer_id_final ,order_id ,rank() over (partition by customer_id_final, order_date order by order_date, order_id) as rw from rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV ) AS B Where A.order_id = B.order_id; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id_final , min(order_Date) OVER ( partition BY customer_id_final) ACQUISITION_DATE from rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV B where lower(order_status) not in (\'cancelled\',\'returned\') ) AS B where A.customer_id_final = B.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = ACQUISITION_DATE and lower(order_status) not in (\'cancelled\',\'returned\') then \'New\' WHEN Order_Date < ACQUISITION_DATE THEN \'Yet to make completed order\' ELSE \'Repeat\' END AS Flag FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and lower(order_status) not in (\'cancelled\',\'returned\') THEN \'New\' WHEN new_customer_flag IS NULL and lower(order_status) in (\'cancelled\',\'returned\') THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(acquisition_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(acquisition_date, \'month\') THEN \'Yet to make completed order\' ELSE \'Repeat\' END AS Flag FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and lower(order_status) not in (\'cancelled\',\'returned\') THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE rpsg_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel , marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, ga_channel as channel, marketplace, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV where lower(order_status) not in (\'cancelled\',\'returned\')) res WHERE order_date=firstorderdate; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS a SET a.acquisition_channel=b.channel FROM rpsg_db.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS a SET a.acquisition_marketplace=b.marketplace FROM rpsg_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE rpsg_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, productname, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, productname, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV where lower(order_status) not in (\'cancelled\',\'returned\'))res WHERE order_date=firstorderdate; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV AS A SET A.acquisition_product=B.productname FROM ( SELECT * FROM rpsg_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final;",
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
                        