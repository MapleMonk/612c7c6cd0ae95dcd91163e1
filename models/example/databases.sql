{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.maplemonk.unicommerce_fact_items_intermediate_final as with order_related as ( select replace(saleorderdto:channel,\'\"\',\'\') as marketplace, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:displayOrderCode,\'\"\',\'\') as marketplace_order_id, replace(saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(saleorderdto:billingAddress:name,\'\"\',\'\') as name, saleorderdto:billingAddress:email as email, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:updated,\'1970-01-01\')) SHIPPING_LAST_UPDATE_DATE, replace(A.Value:itemSku,\'\"\',\'\') as sku, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(A.Value:itemName,\'\"\',\'\') as product_name, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, upper(replace(saleorderdto:billingAddress:city,\'\"\',\'\')) as city, upper(replace(saleorderdto:billingAddress:state,\'\"\',\'\')) as state, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_date, A.Value:shippingCharges::float as shipping_price, A.Value:packetNumber::int as SUBORDER_QUANTITY, A.Value:discount::float as discount, A.Value:totalIntegratedGst::float as tax, A.Value:totalPrice::float as SELLING_PRICE, replace(A.Value:shippingPackageCode,\'\"\',\'\') as shippingPackageCode, replace(A.Value:shippingPackageStatus,\'\"\',\'\') as shippingPackageStatus, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, A.Value:id as SALES_ORDER_ITEM_ID, replace(B.Value:trackingNumber,\'\"\',\'\') as AWB, case when replace(saleorderdto:cod,\'\"\',\'\') = \'true\' then \'COD\' else \'Prepaid\' end as payment_method from XYXX_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A, lateral flatten (INPUT => saleorderdto:shippingPackages)B ) , shipping_related as ( select replace(A.Value:shippingProvider,\'\"\',\'\') as courier, replace(A.Value:status,\'\"\',\'\') shipping_status, A.Value:dispatched dispatched, A.Value:delivered delivered, A.Value:code as shippingPackageCode, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from XYXX_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST c, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A ) , pincode as ( select order_id, pincode from (select replace(c.saleorderdto:code,\'\"\',\'\') as order_id, replace(A.value:pincode,\'\"\',\'\') pincode, row_number() over (partition by order_id order by 1) rw from XYXX_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST C , LATERAL FLATTEN (INPUT => saleorderdto:addresses)A )where rw = 1 ) , returns as ( select B.Value:saleOrderItemCode as saleOrderItemCode, B.Value:itemSku as itemSku, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from XYXX_db.maplemonk.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST C , LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) select o.*,s.courier, s.shipping_status, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.dispatched,\'1970-01-01\')) Dispatch_date, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.delivered,\'1970-01-01\'))) Delivered_Date, case when r.itemSku is not NUll then 1 else 0 end as return_flag, case when return_flag = 1 then suborder_quantity else 0 end::int as return_quantity, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE( product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, case when order_status=\'COMPLETE\' then delivered_date-order_date::date else current_date - order_date::date end as days_in_shipment, p.pincode from order_related o left join shipping_related s on o.shippingPackageCode= s.shippingPackageCode and o.order_id=s.order_id left join returns r on r.saleOrderItemCode = o.saleOrderItemCode and r.order_id = o.order_id left join pincode p on p.order_id = o.order_id ; create or replace table XYXX_db.maplemonk.customerID_test_Final as with new_phone_numbers as ( select contact_num ,9700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(replace(phone,\' \',\'\'), \'[^a-zA-Z0-9]+\'),10) as contact_num from XYXX_db.maplemonk.unicommerce_fact_items_intermediate_Final ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,9800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from XYXX_db.maplemonk.unicommerce_fact_items_intermediate_final ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num,email,case when email is not null and email <> \'\' then min(maple_monk_id) over (partition by email ) else maple_monk_id end maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table XYXX_db.maplemonk.unicommerce_fact_items_XYXX_Final as select coalesce(c.maple_monk_id,c.maple_monk_id) customer_id, o.*, min(order_date) over(partition by customer_id) as acquisition_date from XYXX_db.maplemonk.unicommerce_fact_items_intermediate_final o left join (select distinct contact_num phone,maple_monk_id from XYXX_db.maplemonk.customerID_test_final )c on replace(c.phone,\' \',\'\') = replace(o.phone,\' \',\'\'); CREATE OR REPLACE TABLE XYXX_db.maplemonk.unicommerce_fact_items_XYXX_TEMP_Category as select fi.*,coalesce(p.SKUCODE,fi.SKU) AS SKU_CODE,upper(coalesce(p.name,fi.product_name)) as PRODUCT_NAME_Final,Upper(p.CATEGORY) AS Product_Category, Upper(p.product_sub_Category) product_sub_category, upper(p.collection) collection from XYXX_db.maplemonk.UNICOMMERCE_FACT_ITEMS_XYXX_FINAL fi left join (select * from (select skucode, name, category, collection, product_sub_category, row_number() over (partition by skucode order by 1) rw from XYXX_DB.maplemonk.sku_master) where rw = 1 )p on fi.sku = p.skucode; CREATE OR REPLACE TABLE XYXX_db.maplemonk.unicommerce_fact_items_XYXX_TEMP_Category as select tfi.*,upper(q.super_category) as Product_Super_Category from unicommerce_fact_items_XYXX_TEMP_Category tfi left join (select distinct category, super_category from XYXX_DB.maplemonk.product_type_category_mapping) q on lower(tfi.product_category)=lower(q.category); CREATE OR REPLACE TABLE XYXX_db.maplemonk.unicommerce_fact_items_XYXX_Final AS SELECT * FROM XYXX_db.maplemonk.unicommerce_fact_items_XYXX_TEMP_Category;",
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
                        