{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ALMOWEAR_DB.maplemonk.sales_consolidated_intermediate_AW as select CUSTOMER_ID, SHOP_NAME, CARRIER_ID, COURIER, EMAIL, PHONE, MARKETPLACE, MARKETPLACE_ID, ORDER_ID, INVOICE_ID, REFERENCE_CODE, MANIFEST_DATE, SHIPPING_LAST_UPDATE_DATE, SHIPPING_STATUS, SKU, SKU_TYPE, PRODUCT_ID, PRODUCTNAME, CURRENCY, IS_REFUND, CITY, STATE, ORDER_STATUS, ORDER_DATE, SHIPPING_PRICE, NUMBER_OF_PRODUCTS_IN_COMBO, SUBORDER_QUANTITY, SHIPPED_QUANTITY, RETURNED_QUANTITY, CANCELLED_QUANTITY, RETURN_SALES, CANCEL_SALES, TAX, SUBORDER_MRP, PRODUCT_MRP, RANGE, CATEGORY, STYLE, COLLECTION, DISCOUNT, SELLING_PRICE, coalesce((coalesce(suborder_mrp::float, product_mrp::float)*(suborder_quantity)),selling_price) MRP_SALES, case when (mrp_sales is null or mrp_sales<selling_price) then DISCOUNT else mrp_sales-selling_price end Discount_MRP, NEW_CUSTOMER_FLAG, WAREHOUSE_NAME, DAYS_IN_SHIPMENT, PAYMENT_MODE, IMPORT_DATE, LAST_UPDATE_DATE, Source, Easy_Ecom_Sync_Flag from( with a as ( select order_date::date as order_date, sku, max(suborder_mrp) as mrp from ALMOWEAR_DB.maplemonk.easy_ecom_consolidated_AW group by 1,2 ) select CUSTOMER_ID, b.SHOP_NAME, coalesce(carrier_id ,NULL) as carrier_id, coalesce(courier ,NULL) as courier, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, case when b.shop_name = \'Amazon\' then \'Amazon.in\' else \'Shopify_India\' end as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, coalesce(c.invoice_id,b.ORDER_ID) as Invoice_id, coalesce(c.reference_code,b.order_name) reference_code, c.manifest_date::datetime as manifest_date, coalesce(shipping_last_update_date::datetime ,NULL) as shipping_last_update_date, coalesce(shipping_status ,NULL) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, b.PRODUCT_NAME as PRODUCTNAME, b.CURRENCY, b.IS_REFUND, b.CITY::varchar City, b.STATE:: varchar State, coalesce(c.order_status,b.order_status) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int as shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, case when b.is_refund = 1 then total_sales end as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, b.TAX::float Tax, a.mrp::float as suborder_mrp, b.mrp::float as product_mrp, b.range, b.category, b.style, b.collection, case when lower(b.shop_name)=\'amazon\' then b.discount::float else discount_before_tax end as discount, ifnull(total_sales,0) as selling_price, case when customer_flag = \'New\' then 1 else 0 end as new_customer_flag, coalesce(c.warehouse_name,\'NA\') as Warehouse_Name, case when shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(b.ORDER_TIMESTAMP::datetime), getdate()) when shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(b.ORDER_TIMESTAMP::datetime),date(shipping_Last_update_date)) end::int as Days_in_Shipment, payment_mode, import_date::datetime as import_date, last_update_date, final_utm_channel as Source, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as Easy_Ecom_Sync_Flag from almowear_db.maplemonk.FACT_ITEMS b left join a on a.order_date = b.order_timestamp::date and a.sku= b.sku left join (select * from ( select *,row_number()over(partition by reference_code,sku order by last_update_date desc) rw from almowear_db.maplemonk.easy_ecom_consolidated_AW ) a where rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.sku = c.sku ) union all select NULL as customer_id, *, MARKETPLACE as SOURCE, \'Synced\' as Easy_Ecom_Sync_Flag from ALMOWEAR_DB.maplemonk.easy_ecom_consolidated_AW where lower(marketplace) not in (\'shopify\',\'gofynd\',\'amazon.in\'); create or replace table ALMOWEAR_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from ALMOWEAR_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_AW ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from ALMOWEAR_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_AW ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table ALMOWEAR_DB.maplemonk.sales_consolidated_AW as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from ALMOWEAR_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_AW o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from ALMOWEAR_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from ALMOWEAR_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE ALMOWEAR_DB.maplemonk.sales_consolidated_AW drop COLUMN new_customer_flag ; ALTER TABLE ALMOWEAR_DB.maplemonk.sales_consolidated_AW ADD COLUMN new_customer_flag varchar(50); ALTER TABLE ALMOWEAR_DB.maplemonk.sales_consolidated_AW ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE ALMOWEAR_DB.maplemonk.sales_consolidated_AW ADD COLUMN acquisition_channel varchar(16777216); UPDATE ALMOWEAR_DB.maplemonk.sales_consolidated_AW AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date <> Min(Order_Date) OVER ( partition BY customer_id_final) THEN \'0\' ELSE \'1\' END AS Flag FROM ALMOWEAR_DB.maplemonk.sales_consolidated_AW)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE ALMOWEAR_DB.maplemonk.sales_consolidated_AW SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'1\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE ALMOWEAR_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, shop_name FROM ( SELECT DISTINCT customer_id_final, order_date, shop_name, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM ALMOWEAR_DB.maplemonk.sales_consolidated_AW ) res WHERE order_date=firstorderdate; UPDATE ALMOWEAR_DB.maplemonk.sales_consolidated_AW AS a SET a.acquisition_channel=b.shop_name FROM ALMOWEAR_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE ALMOWEAR_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, productname, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, productname, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM ALMOWEAR_DB.maplemonk.sales_consolidated_AW )res WHERE order_date=firstorderdate; UPDATE ALMOWEAR_DB.maplemonk.sales_consolidated_AW AS A SET A.acquisition_product=B.productname FROM ( SELECT * FROM ALMOWEAR_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ALMOWEAR_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        