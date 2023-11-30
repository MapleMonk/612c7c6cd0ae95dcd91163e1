{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_drv as select b.SHOP_NAME, coalesce(c.carrier_id ,NULL) as carrier_id, upper(coalesce(b.SHOPIFY_COURIER,c.courier ,NULL)) as courier, upper(coalesce(b.name ,c.Customer_Name)) as Customer_Name, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, upper(b.SHOP_NAME) as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, line_item_id::varchar as Invoice_ID, order_name as Reference_Code, coalesce(c.manifest_date::datetime,NULL) as manifest_date, coalesce(b.SHOPIFY_SHIPPING_UPDATED_DATE,c.shipping_last_update_date::datetime,NULL) as shipping_last_update_date, upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status ,NULL)) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, upper(b.PRODUCT_NAME) as PRODUCTNAME, b.CURRENCY, coalesce(c.IS_REFUND, b.IS_REFUND) as RETURN_FLAG, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, upper(coalesce(c.order_status,b.order_status)) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, coalesce(c.return_sales,case when b.is_refund = 1 then total_sales end) as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, coalesce(c.tax, b.TAX::float) Tax, max(c.suborder_mrp) over (partition by b.ORDER_TIMESTAMP::date, b.sku) as suborder_mrp, upper(coalesce(s.Product_Category_Mapped,sp.Product_Category_Mapped,c.category,b.category)) category, upper(coalesce(s.Product_name_mapped,sp.Product_name_mapped)) Product_Name_Mapped, upper(coalesce(s.Report_Category,sp.Report_Category)) Report_Category, upper(coalesce(s.Product_Pack,sp.Product_Pack)) Product_Pack, coalesce(s.product_quantity, sp.product_quantity) Product_quantity, b.discount_before_tax::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, coalesce(c.suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<(selling_price-b.TAX::float)) then b.discount else mrp_sales-selling_price-b.TAX::float end Discount_MRP, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, upper(coalesce(c.warehouse_name,\'NA\')) as Warehouse_Name, case when upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status)) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int AS DAYS_IN_SHIPMENT, upper(FINAL_UTM_CHANNEL) as Channel, upper(coalesce(b.shopifyql_mapped_source,b.gokwik_utm_source)) Shopify_UTM_source, upper(b.GOKWIK_UTM_MEDIUM) Shopify_UTM_Medium, upper(Final_utm_campaign) as UTM_CAMPAIGN, c.payment_mode, c.import_date::datetime as import_date, c.last_update_date, c.invoice_date, c.company_name, c.pin_code, b.final_utm_campaign, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as EasyEcom_Sync_Flag from rpsg_db.maplemonk.FACT_ITEMS_SHOPIFY_DRV b left join (select * from ( select *,row_number()over(partition by reference_code, order_Date order by last_update_date desc) rw from rpsg_db.maplemonk.fact_items_easyecom_drv ) z where z.rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.order_timestamp = c.order_date left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by lower(sku) order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where sku is not null) where rw=1 ) S on lower(b.sku)=lower(s.sku) left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by product_id order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where product_id is not null) where rw=1 ) SP on lower(b.product_id)=lower(sp.product_id) union all select b.SHOP_NAME, coalesce(c.carrier_id ,NULL) as carrier_id, upper(coalesce(b.SHOPIFY_COURIER,c.courier ,NULL)) as courier, upper(coalesce(b.name ,c.Customer_Name)) as Customer_Name, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, upper(b.SHOP_NAME) as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, line_item_id::varchar as Invoice_ID, order_name as Reference_Code, coalesce(c.manifest_date::datetime,NULL) as manifest_date, coalesce(b.SHOPIFY_SHIPPING_UPDATED_DATE,c.shipping_last_update_date::datetime,NULL) as shipping_last_update_date, upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status ,NULL)) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, upper(b.PRODUCT_NAME) as PRODUCTNAME, b.CURRENCY, coalesce(c.IS_REFUND, b.IS_REFUND) as RETURN_FLAG, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, upper(coalesce(c.order_status,b.order_status)) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, coalesce(c.return_sales,case when b.is_refund = 1 then total_sales end) as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, coalesce(c.tax, b.TAX::float) Tax, max(c.suborder_mrp) over (partition by b.ORDER_TIMESTAMP::date, b.sku) as suborder_mrp, upper(coalesce(s.Product_Category_Mapped,sp.Product_Category_Mapped,c.category,b.category)) category, upper(coalesce(s.Product_name_mapped,sp.Product_name_mapped)) Product_Name_Mapped, upper(coalesce(s.Report_Category,sp.Report_Category)) Report_Category, upper(coalesce(s.Product_Pack,sp.Product_Pack)) Product_Pack, coalesce(s.product_quantity, sp.product_quantity) Product_quantity, b.discount_before_tax::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, coalesce(c.suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<(selling_price-b.TAX::float)) then b.discount else mrp_sales-selling_price-b.TAX::float end Discount_MRP, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, upper(coalesce(c.warehouse_name,\'NA\')) as Warehouse_Name, case when upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status)) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int AS DAYS_IN_SHIPMENT, upper(FINAL_UTM_CHANNEL) as Channel, upper(coalesce(b.shopifyql_mapped_source,b.gokwik_utm_source)) Shopify_UTM_source, upper(b.GOKWIK_UTM_MEDIUM) Shopify_UTM_Medium, upper(Final_utm_campaign) as UTM_CAMPAIGN, c.payment_mode, c.import_date::datetime as import_date, c.last_update_date, c.invoice_date, c.company_name, c.pin_code, b.final_utm_campaign, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as EasyEcom_Sync_Flag from rpsg_db.maplemonk.FACT_ITEMS_SHOPIFY_HERBOBUILD b left join (select * from ( select *,row_number()over(partition by reference_code,order_date order by last_update_date desc) rw from rpsg_db.maplemonk.fact_items_easyecom_drv ) z where z.rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.order_timestamp = c.order_date left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by lower(sku) order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where sku is not null) where rw=1 ) S on lower(b.sku)=lower(s.sku) left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by product_id order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where product_id is not null) where rw=1 ) SP on lower(b.product_id)=lower(sp.product_id) union all select b.SHOP_NAME, coalesce(c.carrier_id ,NULL) as carrier_id, upper(coalesce(b.SHOPIFY_COURIER,c.courier ,NULL)) as courier, upper(coalesce(b.name ,c.Customer_Name)) as Customer_Name, coalesce(b.email ,c.email) as email, coalesce(b.phone ,c.contact_num) as phone, upper(b.SHOP_NAME) as MARKETPLACE, coalesce(MARKETPLACE_ID ,NULL) as MARKETPLACE_ID, b.ORDER_ID, line_item_id::varchar as Line_Item_ID, line_item_id::varchar as Invoice_ID, order_name as Reference_Code, coalesce(c.manifest_date::datetime,NULL) as manifest_date, coalesce(b.SHOPIFY_SHIPPING_UPDATED_DATE,c.shipping_last_update_date::datetime,NULL) as shipping_last_update_date, upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status ,NULL)) as shipping_status, coalesce(b.sku ,c.sku) as sku, coalesce(c.sku_type ,NULL) as sku_type, b.PRODUCT_ID, upper(b.PRODUCT_NAME) as PRODUCTNAME, b.CURRENCY, coalesce(c.IS_REFUND, b.IS_REFUND) as RETURN_FLAG, upper(b.CITY::varchar) City, upper(b.STATE:: varchar) State, upper(coalesce(c.order_status,b.order_status)) as Order_Status, b.ORDER_TIMESTAMP::datetime as ORDER_Date, b.shipping_price::float as SHIPPING_PRICE, coalesce(c.number_of_products_in_combo ,NULL) as number_of_products_in_combo, b.quantity::int suborder_quantity, b.quantity::int shipped_quantity, case when b.is_refund = 1 then b.quantity::int end returned_quantity, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, coalesce(c.return_sales,case when b.is_refund = 1 then total_sales end) as return_sales, case when b.is_refund = 0 and lower(b.order_status) in (\'cancelled\') then total_sales end as cancel_sales, coalesce(c.tax, b.TAX::float) Tax, max(c.suborder_mrp) over (partition by b.ORDER_TIMESTAMP::date, b.sku) as suborder_mrp, upper(coalesce(s.Product_Category_Mapped,sp.Product_Category_Mapped,c.category,b.category)) category, upper(coalesce(s.Product_name_mapped,sp.Product_name_mapped)) Product_Name_Mapped, upper(coalesce(s.Report_Category,sp.Report_Category)) Report_Category, upper(coalesce(s.Product_Pack,sp.Product_Pack)) Product_Pack, coalesce(s.product_quantity, sp.product_quantity) Product_quantity, b.discount_before_tax::float as discount, case when b.total_sales::float is null then 0 else b.total_sales::float end selling_price, coalesce(c.suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<(selling_price-b.TAX::float)) then b.discount else mrp_sales-selling_price-b.TAX::float end Discount_MRP, case when b.new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, case when b.new_customer_flag_month = \'New\' then 1 else 0 end as new_customer_flag_month, upper(coalesce(c.warehouse_name,\'NA\')) as Warehouse_Name, case when upper(coalesce(b.SHOPIFY_SHIPPING_STATUS, c.shipping_status)) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_TIMESTAMP),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_TIMESTAMP), getdate()) end::int AS DAYS_IN_SHIPMENT, upper(FINAL_UTM_CHANNEL) as Channel, upper(coalesce(b.shopifyql_mapped_source,b.gokwik_utm_source)) Shopify_UTM_source, upper(b.GOKWIK_UTM_MEDIUM) Shopify_UTM_Medium, upper(Final_utm_campaign) as UTM_CAMPAIGN, c.payment_mode, c.import_date::datetime as import_date, c.last_update_date, c.invoice_date, c.company_name, c.pin_code, b.final_utm_campaign, case when c.reference_code is null then \'Not Synced\' else \'Synced\' end as EasyEcom_Sync_Flag from rpsg_db.maplemonk.FACT_ITEMS_SHOPIFY_AYURVEDICSOURCE b left join (select * from ( select *,row_number()over(partition by reference_code,order_date order by last_update_date desc) rw from rpsg_db.maplemonk.fact_items_easyecom_drv ) z where z.rw = 1 ) c on replace(b.order_name,\'#\',\'\') = c.reference_code and b.order_timestamp = c.order_date left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by lower(sku) order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where sku is not null) where rw=1 ) S on lower(b.sku)=lower(s.sku) left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by product_id order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where product_id is not null) where rw=1 ) SP on lower(b.product_id)=lower(sp.product_id) union all select SHOP_NAME ,CARRIER_ID ,COURIER ,CUSTOMER_NAME ,EMAIL ,contact_num ,MARKETPLACE ,MARKETPLACE_ID ,ORDER_ID ,SUBORDER_ID ,INVOICE_ID ,REFERENCE_CODE ,MANIFEST_DATE ,SHIPPING_LAST_UPDATE_DATE ,upper(SHIPPING_STATUS) SHIPPING_STATUS ,b.SKU ,b.SKU_TYPE ,b.PRODUCT_ID ,b.PRODUCTNAME ,CURRENCY ,IS_REFUND ,CITY ,STATE ,ORDER_STATUS ,ORDER_DATE ,SHIPPING_PRICE ,NUMBER_OF_PRODUCTS_IN_COMBO ,SUBORDER_QUANTITY ,SHIPPED_QUANTITY ,RETURNED_QUANTITY ,CANCELLED_QUANTITY ,RETURN_SALES ,CANCEL_SALES ,TAX ,SUBORDER_MRP ,upper(coalesce(s.Product_Category_Mapped,sp.Product_Category_Mapped,b.category)) category ,upper(coalesce(s.Product_name_mapped,sp.Product_name_mapped,b.PRODUCTNAME)) Product_Name_Mapped ,upper(coalesce(s.Report_Category,sp.Report_Category)) Report_Category ,upper(coalesce(s.Product_Pack,sp.Product_Pack)) Product_Pack ,coalesce(s.product_quantity, sp.product_quantity) Product_quantity ,DISCOUNT ,SELLING_PRICE ,MRP_SALES ,DISCOUNT_MRP ,NEW_CUSTOMER_FLAG ,NEW_CUSTOMER_FLAG_MONTH ,WAREHOUSE_NAME ,case when upper(shipping_STATUS) in (\'DELIVERED\',\'DELIVERED TO ORIGIN\', \'RETURNED\', \'RTO\') then datediff(day,date(b.ORDER_DATE),date(shipping_Last_update_date)) else datediff(day,date(b.ORDER_DATE), getdate()) end::int AS DAYS_IN_SHIPMENT ,CHANNEL ,NULL as Shopify_UTM_source ,NULL as Shopify_UTM_Medium ,NULL AS UTM_CAMPAIGN ,PAYMENT_MODE ,IMPORT_DATE ,LAST_UPDATE_DATE ,invoice_date ,company_name ,b.pin_code ,NULL as FINAL_UTM_CAMPAIGN ,\'Synced\' as EasyEcom_Sync_Flag from rpsg_DB.maplemonk.fact_items_easyecom_drv b left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by lower(sku) order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where sku is not null) where rw=1 ) S on lower(b.sku)=lower(s.sku) left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by product_id order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where product_id is not null) where rw=1 ) SP on lower(b.product_id)=lower(sp.product_id) where not(lower(b.marketplace) like \'%shopify%\'); create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_drv as select *, upper(b.\"Mapped Status\") Final_Status from RPSG_DB.maplemonk.sales_consolidated_intermediate_drv a left join (select distinct status, \"Mapped Status\" from rpsg_db.maplemonk.shipment_status_mapping) b on lower(case when lower(a.order_status) in (\'cancelled\', \'returned\') then a.order_status else coalesce(a.shipping_status, a.order_status) end)= lower(b.status); create or replace table RPSG_DB.maplemonk.sales_consolidated_intermediate_drv as select upper(coalesce(GAC.channel, GAC.GA_SOURCEMEDIUM)) as GA_Channel ,upper(GAC.GA_SOURCE) GA_SOURCE ,upper(GAC.GA_MEDIUM) GA_MEDIUM ,GAC.VIEW_ID ,upper(coalesce(case when lower(SCID.channel) like \'%considerga%\' then GA_CHANNEL else SCID.channel end, GA_CHANNEL,\'NA\')) FINAL_CHANNEL ,SCID.* from RPSG_DB.maplemonk.sales_consolidated_intermediate_drv SCID left join (select * from (select *, Row_number() OVER (partition BY ga_transactionid ORDER BY GA_DATE DESC) rw from RPSG_DB.maplemonk.GA_FINAL_ORDER_BY_SOURCE_CONSOLIDATED_DRV) where rw=1) GAC on replace(SCID.reference_code,\'#\',\'\') =replace(GAC.GA_TRANSACTIONID,\'#\',\'\') ; create or replace table rpsg_DB.maplemonk.Final_customerID as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_DRV ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_DRV ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id_final, min(order_date) over(partition by customer_id_final) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from rpsg_DB.maplemonk.SALES_CONSOLIDATED_INTERMEDIATE_DRV o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from rpsg_DB.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from rpsg_DB.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre drop COLUMN new_customer_flag ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN new_customer_flag varchar(50); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre drop COLUMN new_customer_flag_month ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN new_customer_flag_month varchar(50); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN acquisition_channel varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN acquisition_marketplace varchar(16777216); ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre drop COLUMN ACQUISITION_DATE ; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN ACQUISITION_DATE timestamp; ALTER TABLE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ADD COLUMN SAME_DAY_ORDERNO number; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS A SET A.SAME_DAY_ORDERNO = B.rw FROM ( select distinct customer_id_final ,order_id ,rank() over (partition by customer_id_final, order_date order by order_date, order_id) as rw from rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ) AS B Where A.order_id = B.order_id; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS A SET A.ACQUISITION_DATE = B.ACQUISITION_DATE FROM ( select distinct customer_id_final , min(order_Date) OVER ( partition BY customer_id_final) ACQUISITION_DATE from rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre B where (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) ) AS B where A.customer_id_final = B.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Order_Date = ACQUISITION_DATE and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) then \'New\' WHEN Order_Date < ACQUISITION_DATE or acquisition_date is null THEN \'Yet to make completed order\' WHEN Order_Date > ACQUISITION_DATE then \'Repeat\' END AS Flag FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre )AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final AND A.order_date::date=B.Order_date::Date; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre SET new_customer_flag = CASE WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) THEN \'New\' WHEN new_customer_flag IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) in (\'cancelled\',\'returned\') end) THEN \'Yet to make completed order\' ELSE new_customer_flag END; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS A SET A.new_customer_flag_month = B.flag FROM ( SELECT DISTINCT order_id, customer_id_final, Order_Date, CASE WHEN Last_day(order_date, \'month\') = Last_day(acquisition_date, \'month\') THEN \'New\' WHEN Last_day(order_date, \'month\') < Last_day(acquisition_date, \'month\') or acquisition_date is null THEN \'Yet to make completed order\' WHEN Last_day(order_date, \'month\') > Last_day(acquisition_date, \'month\') THEN \'Repeat\' END AS Flag FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre)AS B WHERE A.order_id = B.order_id AND A.customer_id_final = B.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre SET new_customer_flag_month = CASE WHEN new_customer_flag_month IS NULL and (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end) THEN \'New\' ELSE new_customer_flag_month END; CREATE OR replace temporary TABLE rpsg_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id_final, channel , marketplace FROM ( SELECT DISTINCT customer_id_final, order_date, FINAL_CHANNEL channel, marketplace, Min(order_date) OVER ( partition BY customer_id_final) firstOrderdate FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre where (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end)) res WHERE order_date=firstorderdate; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS a SET a.acquisition_channel=b.channel FROM rpsg_db.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS a SET a.acquisition_marketplace=b.marketplace FROM rpsg_DB.maplemonk.temp_source_1 b WHERE a.customer_id_final = b.customer_id_final; CREATE OR replace temporary TABLE rpsg_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id_final, category, Row_number() OVER (partition BY customer_id_final ORDER BY SELLING_PRICE DESC) rowid FROM ( SELECT DISTINCT customer_id_final, order_date, category, SELLING_PRICE , Min(order_date) OVER (partition BY customer_id_final) firstOrderdate FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre where (case when lower(order_status) is null then 1=1 else lower(order_status) not in (\'cancelled\',\'returned\') end) and (case when lower(shipping_status) is null then 1=1 else lower(shipping_status) not in (\'cancelled\',\'returned\') end))res WHERE order_date=firstorderdate; UPDATE rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre AS A SET A.acquisition_product=B.category FROM ( SELECT * FROM rpsg_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id_final = B.customer_id_final; create or replace table rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV as select *, datediff(month, acquisition_date::date, ordeR_date::date) months_from_acquisition from rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV_pre ; create or replace table RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_drv as select A.* ,coalesce(s.Product_Category_Mapped, sp.Product_Category_Mapped) as category ,coalesce(s.Product_name_mapped,sp.Product_name_mapped) Product_Name_Mapped ,coalesce(s.Report_Category,sp.Report_Category) report_category ,coalesce(s.product_pack,sp.product_pack) Product_Pack ,coalesce(s.Product_Quantity, sp.Product_Quantity) Product_Quantity from (select ORDER_ID ,INVOICE_ID ,RI.VALUE:\"suborder_id\" SUBORDER_ID ,REFERENCE_CODE ,CREDIT_NOTE_ID ,CREDIT_NOTE_NUMBER ,try_to_timestamp(ORDER_DATE) ORDER_DATE ,try_to_timestamp(INVOICE_DATE) INVOICE_DATE ,try_to_timestamp(RETURN_DATE) RETURN_DATE ,try_to_timestamp(MANIFEST_DATE) MANIFEST_DATE ,try_to_timestamp(IMPORT_DATE) IMPORT_DATE ,try_to_timestamp(LAST_UPDATE_DATE) LAST_UPDATE_DATE ,RI.VALUE:company_product_id COMPANY_PRODUCT_ID ,replace(RI.VALUE:productName,\'\"\',\'\') PRODUCTNAME ,RI.VALUE:product_id PRODUCT_ID ,replace(RI.VALUE:sku,\'\"\',\'\') SKU ,MARKETPLACE ,COMPANY_NAME ,MARKETPLACE_ID ,REPLACEMENT_ORDER ,replace(RI.VALUE:return_reason,\'\"\',\'\') RETURN_REASON ,ifnull(RI.VALUE:returned_item_quantity::float,0) RETURNED_QUANTITY ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) RETURN_AMOUNT_WITHOUT_TAX ,ifnull(RI.Value:credit_note_total_item_tax::float,0) RETURN_TAX ,ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0) RETURN_SHIPPING_CHARGE ,ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) RETURN_MISC ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) + ifnull(RI.Value:credit_note_total_item_tax::float,0) + ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0)+ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) TOTAL_RETURN_AMOUNT ,row_number() over (partition by credit_note_number, order_id, suborder_id, invoice_id, company_product_id order by last_update_date desc) rw from RPSG_DB.MAPLEMONK.easyecom_vl_returns R, LATERAL flatten(INPUT => R.ITEMS) RI ) A left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by lower(sku) order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where sku is not null) where rw=1 ) S on lower(A.sku)=lower(s.sku) left join (select * from (select sku, product_id, productname, Product_name_mapped, \"Product Category\" Product_Category_Mapped, \"Report Category\" Report_Category, \"Product Pack\" Product_Pack, \"Product Quantity\" Product_Quantity, row_number() over (partition by product_id order by \"Product Category\") rw from rpsg_DB.maplemonk.new_sku_master where product_id is not null) where rw=1 ) SP on lower(A.product_id)=lower(sp.product_id) where A.rw=1 ; Create or replace table RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_drv as select ifnull(FE.channel,\'NA\') CHANNEL ,FE.Customer_id_final ,FE.phone ,FR.* from rpsg_db.maplemonk.fact_items_easyecom_returns_detailed_drv FR left join (select * from (select distinct replace(reference_code,\'#\',\'\') REFERENCE_CODE, FINAL_CHANNEL channel, customer_id_final, phone, row_number() over (partition by replace(reference_code,\'#\',\'\') order by 1) rw from RPSG_DB.MAPLEMONK.sales_consolidated_drv) where rw=1) FE on FR.REFERENCE_CODE = FE.REFERENCE_CODE; create or replace table RPSG_DB.MAPLEMONK.EASYECOM_RETURNS_SUMMARY_DRV as select MARKETPLACE ,COMPANY_NAME ,MARKETPLACE_ID ,Return_Date ,CHANNEL ,sum(RETURNED_QUANTITY) TOTAL_RETURNED_QUANTITY ,sum(TOTAL_RETURN_AMOUNT) TOTAL_RETURN_AMOUNT ,sum(RETURN_TAX) TOTAL_RETURN_TAX ,sum(RETURN_AMOUNT_WITHOUT_TAX) TOTAL_RETURN_AMOUNT_EXCL_TAX from RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_drv group by 1,2,3,4,5 order by 4 desc;",
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
                        