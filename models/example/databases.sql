{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ugaoo_db.maplemonk.unicommerce_fact_items_intermediate as with order_related as ( select replace(saleorderdto:channel,\'\"\',\'\') as marketplace, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(saleorderdto:billingAddress:name,\'\"\',\'\') as name, saleorderdto:billingAddress:email as email, date(saleorderdto:updated) as SHIPPING_LAST_UPDATE_DATE, replace(A.Value:itemSku,\'\"\',\'\') as sku, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(A.Value:itemName,\'\"\',\'\') as product_name, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, replace(saleorderdto:billingAddress:city,\'\"\',\'\') as city, replace(saleorderdto:billingAddress:state,\'\"\',\'\') as state, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, date(saleorderdto:displayOrderDateTime) as order_date, A.Value:shippingCharges::float as shipping_price, A.Value:packetNumber::int as SUBORDER_QUANTITY, A.Value:discount::float as discount, A.Value:totalIntegratedGst::float as tax, A.Value:totalPrice::float as SELLING_PRICE, replace(A.Value:shippingPackageCode,\'\"\',\'\') as shippingPackageCode, replace(A.Value:shippingPackageStatus,\'\"\',\'\') as shippingPackageStatus, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, A.Value:id as SALES_ORDER_ITEM_ID from UGAOO_DB.MAPLEMONK.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A ), shipping_related as ( select replace(A.Value:shippingProvider,\'\"\',\'\') as courier, replace(A.Value:status,\'\"\',\'\') shipping_status, A.Value:dispatched dispatched, A.Value:delivered delivered, A.Value:code as shippingPackageCode from UGAOO_DB.MAPLEMONK.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A ), returns as ( select distinct B.Value:saleOrderItemCode as saleOrderItemCode, B.Value:itemSku as itemSku from UGAOO_DB.MAPLEMONK.UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) select o.*,s.courier, s.shipping_status, date(s.dispatched) Dispatch_date, date(s.delivered) Delivered_Date, case when r.itemSku is not NUll then 1 else 0 end as return_flag, case when return_flag = 1 then suborder_quantity else 0 end::int as return_quantity, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE( product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, case when order_status=\'COMPLETE\' then delivered_date-order_date else current_date - order_date end as days_in_shipment from order_related o left join shipping_related s on o.shippingPackageCode= s.shippingPackageCode left join returns r on r.saleOrderItemCode = o.saleOrderItemCode; create or replace table ugaoo_db.maplemonk.customerID_test as with new_phone_numbers as ( select contact_num ,9700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num from ugaoo_db.maplemonk.unicommerce_fact_items ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,9800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from ugaoo_db.maplemonk.unicommerce_fact_items ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num,email,case when email is not null and email <> \'\' then min(maple_monk_id) over (partition by email ) else maple_monk_id end maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table ugaoo_db.maplemonk.unicommerce_fact_items as select coalesce(c.maple_monk_id,c.maple_monk_id) customer_id, o.*, min(order_date) over(partition by customer_id) as acquisition_date from ugaoo_db.maplemonk.unicommerce_fact_items_intermediate o left join (select distinct contact_num phone,maple_monk_id from ugaoo_db.maplemonk.customerID_test )c on c.phone = o.phone; create or replace table ugaoo_db.maplemonk.unicommerce_fact_items_category_mapping as select fi.* ,usm.category as Product_category ,usm.mrp as MRP ,usm.weight as Weight ,usm.\"Cost Price\" as Cost_Price ,usm.dimensions as Dimensions ,usm.type as Product_Type from ugaoo_db.maplemonk.unicommerce_fact_items fi left join ugaoo_db.maplemonk.unicommerce_sku_mapping usm on fi.sku = usm.\"Sku Code\"; create or replace table ugaoo_db.maplemonk.unicommerce_fact_items as select * from ugaoo_db.maplemonk.unicommerce_fact_items_category_mapping; create or replace table ugaoo_db.maplemonk.asp_in_consolidated as select null as customer_id, \'Amazon\' as Marketplace, \'Amazon\' as Source, ain.\"amazon-order-id\" as order_id, null as phone, null as name, TO_VARIANT(null) as email, cast(ain.\"last-updated-date\" as date) as SHIPPING_LAST_UPDATE_DATE, ain.SKU, ain.ASIN as Product_id, ain.\"product-name\" as Product_name, ain.CURRENCY, ain.\"ship-city\" as city, ain.\"ship-state\" as state, ain.\"order-status\" as order_status, cast(ain.\"purchase-date\" as date) as order_date, sum(cast(case when ain.\"shipping-price\" = \'\' then 0 else ain.\"shipping-price\" end as float)) as Shipping_Price, sum(cast(case when ain.QUANTITY = \'\' then 0 else ain.QUANTITY end as float)) as suborder_quantity, sum(cast(case when ain.\"item-promotion-discount\" = \'\' then 0 else ain.\"item-promotion-discount\" end as float)) as Discount, sum(cast(case when ain.\"item-tax\" = \'\' then 0 else ain.\"item-tax\" end as float)) as Tax, sum(cast(case when ain.\"item-price\" = \'\' then 0 else ain.\"item-price\" end as float)) as Selling_price, null as SHIPPINGPACKAGECODE, null as SHIPPINGPACKAGESTATUS, null as SALEORDERITEMCODE, TO_VARIANT(null) as SALES_ORDER_ITEM_ID, ain.\"fulfillment-channel\" as Courier, null as Shipping_status, null as dispatch_date, null as delivered_date, 0 as return_flag, 0 as return_quantity, 0 as cancelled_quantity, null as new_customer_flag, null as acquisition_product, null as days_in_shipment, null as acqusition_date, usm.category as Product_Category, usm.mrp as MRP, usm.weight as Weight, usm.\"Cost Price\" as Cost_Price, usm.dimensions as Dimensions, usm.type as Product_Type from UGAOO_DB.MAPLEMONK.ASP_IN_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL ain left join ugaoo_db.maplemonk.unicommerce_sku_mapping usm on ain.sku = usm.\"Sku Code\" where ain.\"item-status\" <> \'Cancelled\' group by ain.\"amazon-order-id\", cast(ain.\"last-updated-date\" as date), ain.SKU, ain.ASIN, ain.\"product-name\", ain.CURRENCY, ain.\"ship-city\", ain.\"ship-state\", ain.\"order-status\", cast(ain.\"purchase-date\" as date), ain.\"fulfillment-channel\", usm.category, usm.mrp, usm.weight, usm.\"Cost Price\", usm.dimensions, usm.type; insert into ugaoo_db.maplemonk.unicommerce_fact_items select * from ugaoo_db.maplemonk.asp_in_consolidated;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from UGAOO_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        