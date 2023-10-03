{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table skinq_db.Maplemonk.skinq_db_UNICOMMERCE_fact_items_intermediate_final as with Unicommerce_UCSkinq_order_related as ( select replace(saleorderdto:channel,\'\"\',\'\') as marketplace, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:displayOrderCode,\'\"\',\'\') as order_name, replace(saleorderdto:displayOrderCode,\'\"\',\'\') as reference_code, replace(saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(saleorderdto:billingAddress:name,\'\"\',\'\') as name, saleorderdto:billingAddress:email as email, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:updated,\'1970-01-01\')) SHIPPING_LAST_UPDATE_DATE, replace(A.Value:itemSku,\'\"\',\'\') as sku, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(A.Value:itemName,\'\"\',\'\') as product_name, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, upper(replace(saleorderdto:billingAddress:city,\'\"\',\'\')) as city, upper(replace(saleorderdto:billingAddress:state,\'\"\',\'\')) as state, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_date, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',B.value:invoiceDate,\'1970-01-01\')) as invoice_date, A.Value:shippingCharges::float as shipping_price, A.Value:packetNumber::int as SUBORDER_QUANTITY, A.Value:discount::float as discount, A.Value:totalIntegratedGst::float as tax, A.Value:totalPrice::float as SELLING_PRICE, replace(A.Value:shippingPackageCode,\'\"\',\'\') as shippingPackageCode, replace(A.Value:shippingPackageStatus,\'\"\',\'\') as shippingPackageStatus, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, A.Value:id as SALES_ORDER_ITEM_ID, A.Value:facilityName as warehouse_name, replace(B.Value:trackingNumber,\'\"\',\'\') as AWB, case when replace(saleorderdto:cod,\'\"\',\'\') = \'true\' then \'COD\' else \'Prepaid\' end as payment_mode from skinq_db.maplemonk.UNICOMMERCE_UCSKINQ_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A, lateral flatten (INPUT => saleorderdto:shippingPackages)B ), Unicommerce_UCSkinq_shipping_related as ( select replace(A.Value:shippingProvider,\'\"\',\'\') as courier, replace(A.Value:status,\'\"\',\'\') shipping_status, replace(A.Value:invoice,\'\"\',\'\') invoice_id, A.Value:dispatched dispatched, A.Value:delivered delivered, A.Value:code as shippingPackageCode, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from skinq_db.maplemonk.UNICOMMERCE_UCSKINQ_GET_ORDERS_BY_IDS_TEST c, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A ), Unicommerce_UCSkinq_returns as ( select B.Value:saleOrderItemCode as saleOrderItemCode, B.Value:itemSku as itemSku, replace(c.saleorderdto:code,\'\"\',\'\') as order_id from skinq_db.maplemonk.Unicommerce_UCSkinq_get_orders_by_ids_test C , LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) select o.*,s.courier, s.shipping_status, s.invoice_id, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.dispatched,\'1970-01-01\')) Dispatch_date, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',s.delivered,\'1970-01-01\'))) Delivered_Date, case when r.itemSku is not NUll then 1 else 0 end as return_flag, case when return_flag = 1 then suborder_quantity else 0 end::int as return_quantity, case when return_flag = 1 then selling_price else 0 end::float as return_sales, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE( product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, case when UPPER(order_status)=\'COMPLETE\' then delivered_date-order_date::date else current_date - order_date::date end as days_in_shipment from Unicommerce_UCSkinq_order_related o left join Unicommerce_UCSkinq_shipping_related s on o.shippingPackageCode= s.shippingPackageCode and o.order_id=s.order_id left join Unicommerce_UCSkinq_returns r on r.saleOrderItemCode = o.saleOrderItemCode and r.order_id = o.order_id ; create or replace table skinq_db.Maplemonk.skinq_db_customerID_test_Final as with new_phone_numbers as ( select contact_num ,9700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(replace(phone,\' \',\'\'), \'[^a-zA-Z0-9]+\'),10) as contact_num from skinq_db.Maplemonk.skinq_db_unicommerce_fact_items_intermediate_final ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,9800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select replace(phone,\' \',\'\') as contact_num,email from skinq_db.Maplemonk.skinq_db_unicommerce_fact_items_intermediate_final ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num,email,case when email is not null and email <> \'\' then min(maple_monk_id) over (partition by email ) else maple_monk_id end maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table skinq_db.Maplemonk.skinq_db_unicommerce_fact_items as select coalesce(c.maple_monk_id,c.maple_monk_id) customer_id, o.*, min(order_date) over(partition by customer_id) as acquisition_date from skinq_db.Maplemonk.skinq_db_unicommerce_fact_items_intermediate_final o left join (select distinct contact_num phone,maple_monk_id from skinq_db.Maplemonk.skinq_db_customerID_test_Final )c on replace(c.phone,\' \',\'\') = replace(o.phone,\' \',\'\'); CREATE TABLE IF NOT EXISTS skinq_db.Maplemonk.skinq_db_SKU_MASTER ( skucode VARCHAR(16777216), name VARCHAR(16777216), category VARCHAR(16777216), sub_category VARCHAR(16777216)); CREATE OR REPLACE TABLE skinq_db.Maplemonk.skinq_db_unicommerce_fact_items_TEMP_Category as select fi.*, coalesce(p.primarykey,fi.SKU) AS SKU_CODE, coalesce(p.name,fi.product_name) as PRODUCT_NAME_Final, Upper(p.CATEGORY) AS Product_Category, Upper(p.SUB_CATEGORY) AS Product_Sub_Category from skinq_db.Maplemonk.skinq_db_unicommerce_fact_items fi left join (select * from (select primarykey, \"PRODUCT TITLE\" name, category, sub_category, row_number() over (partition by primarykey order by 1) rw from skinq_db.Maplemonk.skinq_db_sku_master) where rw = 1 ) p on fi.sku = p.primarykey; CREATE OR REPLACE TABLE skinq_db.Maplemonk.skinq_db_unicommerce_fact_items AS SELECT * FROM skinq_db.Maplemonk.skinq_db_unicommerce_fact_items_TEMP_Category;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from skinq_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        