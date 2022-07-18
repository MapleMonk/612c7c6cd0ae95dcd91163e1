{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table FABALLEY_DB.maplemonk.unicommerce_fact_items_intermediate_final as with order_related as ( select B.*, Shipping_price_base_item*\"Currency Rate Admin\" as shipping_price_INR_item, Max_retail_Price_base - SELLING_PRICE_base*\"Currency Rate Admin\" +div0(sum(shipping_price_base_item) over (partition by order_id), count( distinct case when lower(Item_Status) <> \'cancelled\' then SALES_ORDER_ITEM_ID end ) over (partition by order_id))*\"Currency Rate Admin\"- Shipping_price_base_item*\"Currency Rate Admin\" as discount_INR, tax_base*\"Currency Rate Admin\" as tax_INR, STORE_CREDIT_base*\"Currency Rate Admin\" as store_credit_INR, SELLING_PRICE_base*\"Currency Rate Admin\" +div0(sum(shipping_price_base_item) over (partition by order_id), count( distinct case when lower(Item_Status) <> \'cancelled\' then SALES_ORDER_ITEM_ID end ) over (partition by order_id))*\"Currency Rate Admin\"- Shipping_price_base_item*\"Currency Rate Admin\" as Selling_price_INR, Max_retail_Price_base*1 as Max_Retail_Price_INR, div0(sum(shipping_price_base_item) over (partition by order_id), count( distinct case when lower(Item_Status) <> \'cancelled\' then SALES_ORDER_ITEM_ID end ) over (partition by order_id))*\"Currency Rate Admin\" as shipping_price_INR from ( select replace(saleorderdto:channel,\'\"\',\'\') as marketplace, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(saleorderdto:billingAddress:name,\'\"\',\'\') as name, saleorderdto:billingAddress:email as email, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:updated,\'1970-01-01\')) SHIPPING_LAST_UPDATE_DATE, replace(A.Value:itemSku,\'\"\',\'\') as sku, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(A.Value:itemName,\'\"\',\'\') as product_name, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, upper(replace(saleorderdto:billingAddress:country,\'\"\',\'\')) as country, case when country = \'IN\' then \'India\' else \'International\' end as CountryCategory, upper(replace(saleorderdto:billingAddress:city,\'\"\',\'\')) as city, upper(replace(saleorderdto:billingAddress:state,\'\"\',\'\')) as state, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_date, hour(order_date) Order_Hour, A.Value:shippingCharges::float as Shipping_price_base_item, A.Value:packetNumber::int as SUBORDER_QUANTITY, ifnull(A.Value:maxRetailPrice::float,0) - ifnull(A.Value:totalPrice::float,0) - ifnull(A.Value:storeCredit::float,0) as discount_base, A.Value:totalIntegratedGst::float as tax_base, ifnull(A.Value:totalPrice::float,0) + ifnull(A.Value:storeCredit::float,0) as SELLING_PRICE_base, A.Value:storeCredit::float as STORE_CREDIT_base, A.Value:maxRetailPrice::float as Max_retail_Price_base, replace(A.Value:statusCode,\'\"\',\'\') as Item_Status, replace(A.Value:shippingPackageCode,\'\"\',\'\') as shippingPackageCode, replace(A.Value:shippingPackageStatus,\'\"\',\'\') as shippingPackageStatus, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, A.Value:id as SALES_ORDER_ITEM_ID from FABALLEY_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A ) B left join faballey_db.maplemonk.conversion_rates c on c.\"Currency Code\" = b.currency ) , shipping_related as ( select replace(A.Value:shippingProvider,\'\"\',\'\') as courier, replace(A.Value:status,\'\"\',\'\') shipping_status, A.Value:dispatched dispatched, A.Value:delivered delivered, A.Value:code as shippingPackageCode, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from FABALLEY_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST c, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A ) , returns as ( select B.Value:saleOrderItemCode as saleOrderItemCode, B.Value:itemSku as itemSku, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from FABALLEY_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST C , LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) select o.*,s.courier, s.shipping_status, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.dispatched,\'1970-01-01\'))) Dispatch_date, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.delivered,\'1970-01-01\'))) Delivered_Date, case when r.itemSku is not NUll then 1 else 0 end as return_flag, case when return_flag = 1 then suborder_quantity else 0 end::int as return_quantity, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE( product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, case when order_status=\'COMPLETE\' then delivered_date-order_date::date else current_date - order_date::date end as days_in_shipment from order_related o left join shipping_related s on o.shippingPackageCode= s.shippingPackageCode and o.order_id=s.order_id left join returns r on r.saleOrderItemCode = o.saleOrderItemCode and r.order_id = o.order_id ; create or replace table FABALLEY_db.maplemonk.final_customerid as with new_phone_numbers as ( select phone, contact_num ,19700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num, phone from FABALLEY_db.maplemonk.unicommerce_fact_items_intermediate_final ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,19800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from FABALLEY_db.maplemonk.unicommerce_fact_items_intermediate_final ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num, email, maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table FABALLEY_db.maplemonk.unicommerce_fact_items_FABALLEY_Final as select coalesce(m.maple_monk_id_phone, d.maple_monk_id) as customer_id, min(order_date) over(partition by customer_id) as acquisition_date, m.* from (select c.maple_monk_id as maple_monk_id_phone, o.* from FABALLEY_db.maplemonk.unicommerce_fact_items_intermediate_final o left join (select * from (select contact_num phone,maple_monk_id, row_number() over (partition by contact_num order by maple_monk_id asc) magic from Faballey_db.maplemonk.Final_customerID) where magic =1 )c on c.phone = right(regexp_replace(o.phone, \'[^a-zA-Z0-9]+\'),10))m left join (select distinct maple_monk_id, email from Faballey_db.maplemonk.Final_customerID where contact_num is null )d on d.email = m.email ; ALTER TABLE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final drop COLUMN new_customer_flag ; ALTER TABLE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final ADD COLUMN new_customer_flag varchar(50); ALTER TABLE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final drop COLUMN acquisition_product ; ALTER TABLE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final ADD COLUMN acquisition_product varchar(16777216); ALTER TABLE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final ADD COLUMN acquisition_marketplace varchar(16777216); UPDATE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final AS A SET A.new_customer_flag = B.flag FROM ( SELECT DISTINCT order_id, customer_id, Order_Date, CASE WHEN Order_Date <> Min(Order_Date) OVER ( partition BY customer_id) THEN \'Repeat\' ELSE \'New\' END AS Flag FROM FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final)AS B WHERE A.order_id = B.order_id AND A.customer_id = B.customer_id; UPDATE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final SET new_customer_flag = CASE WHEN new_customer_flag IS NULL THEN \'New\' ELSE new_customer_flag END; CREATE OR replace temporary TABLE FABALLEY_DB.maplemonk.temp_source_1 AS SELECT DISTINCT customer_id, marketplace FROM ( SELECT DISTINCT customer_id, order_date, marketplace as marketplace, Min(order_date) OVER ( partition BY customer_id) firstOrderdate FROM FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final ) res WHERE order_date=firstorderdate; UPDATE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final AS a SET a.acquisition_marketplace=b.marketplace FROM FABALLEY_DB.maplemonk.temp_source_1 b WHERE a.customer_id = b.customer_id; CREATE OR REPLACE TABLE FABALLEY_db.maplemonk.unicommerce_fact_items_FABALLEY_TEMP_Category as select fi.*,fi.SKU AS SKU_CODE,coalesce(p.Item_Name,fi.product_name) as PRODUCT_NAME_Final,Upper(p.CATEGORY) AS Product_Category, Upper(p.Color) Color, Upper(p.Size) Size, Upper(P.Brand) Brand from FABALLEY_db.maplemonk.UNICOMMERCE_FACT_ITEMS_FABALLEY_FINAL fi left join (select distinct \"Sku Code\" SKUCODE, \"Item Name\" Item_Name, CATEGORY, Color, Size, BRAND from FABALLEY_DB.maplemonk.unicommerce_faballey_sku_master) p on fi.sku = p.skucode; CREATE OR REPLACE TABLE FABALLEY_db.maplemonk.unicommerce_fact_items_FABALLEY_Final AS SELECT * FROM FABALLEY_db.maplemonk.unicommerce_fact_items_FABALLEY_TEMP_Category; CREATE OR replace temporary TABLE FABALLEY_DB.maplemonk.temp_product_1 AS SELECT DISTINCT customer_id, product_name_final, Row_number() OVER (partition BY customer_id ORDER BY SELLING_PRICE_INR DESC) rowid FROM ( SELECT DISTINCT customer_id, order_date, product_name_final, SELLING_PRICE_INR , Min(order_date) OVER (partition BY customer_id) firstOrderdate FROM FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final )res WHERE order_date=firstorderdate; UPDATE FABALLEY_DB.maplemonk.unicommerce_fact_items_FABALLEY_Final AS A SET A.acquisition_product=B.product_name_final FROM ( SELECT * FROM FABALLEY_DB.maplemonk.temp_product_1 WHERE rowid=1)B WHERE A.customer_id = B.customer_id; create or replace table faballey_db.maplemonk.Fact_item_Website_FabAlley AS Select * from FABALLEY_db.maplemonk.unicommerce_fact_items_FABALLEY_Final where lower(marketplace)=\'custom\';",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from FABALLEY_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        