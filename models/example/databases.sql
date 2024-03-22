{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.shipping_timestamps as select a.order_name, a.order_id, a.shipping_package_code, a.quantity, a.created_timestamp, b.picking_timestamp, c.picked_timestamp, d.packed_timestamp, e.manifested_timestamp, f.dispatched_timestamp from ( (select \"Display Order Code\" order_name, \"Sale Order Code\" order_id, \"Shipping Package Code\" shipping_package_code, \"No. of Items\" quantity, \"Time Stamp\" created_timestamp from snitch_db.maplemonk.shipping_package_timeline where \"New Value\" = \'CREATED\') a left join (select \"Shipping Package Code\" shipping_package_code, \"Time Stamp\" picking_timestamp from snitch_db.maplemonk.shipping_package_timeline where \"New Value\" = \'PICKING\') b on a.shipping_package_code = b.shipping_package_code left join (select \"Shipping Package Code\" shipping_package_code, \"Time Stamp\" picked_timestamp from snitch_db.maplemonk.shipping_package_timeline where \"New Value\" = \'PICKED\') c on a.shipping_package_code = c.shipping_package_code left join (select \"Shipping Package Code\" shipping_package_code, \"Time Stamp\" packed_timestamp from snitch_db.maplemonk.shipping_package_timeline where \"New Value\" = \'PACKED\') d on a.shipping_package_code = d.shipping_package_code left join (select \"Shipping Package Code\" shipping_package_code, \"Time Stamp\" manifested_timestamp from snitch_db.maplemonk.shipping_package_timeline where \"New Value\" = \'MANIFESTED\') e on a.shipping_package_code = e.shipping_package_code left join (select \"Shipping Package Code\" shipping_package_code, \"Time Stamp\" dispatched_timestamp from snitch_db.maplemonk.shipping_package_timeline where \"New Value\" = \'DISPATCHED\') f on a.shipping_package_code = f.shipping_package_code ) ; create or replace table snitch_db.maplemonk.LOGICERPConsolidated_GET_SALE_INVOICE as select Branch_Name, BRANCH_SHORT_NAME, bill_no, new_bill_no, retail_cust_mobile_no, retail_cust_name, retail_cust_email_id, LSTITEMS, bill_cancelled, bill_date from snitch_db.maplemonk.LOGICERPNEW_GET_SALE_INVOICE union all (select Branch_Name, BRANCH_SHORT_NAME, bill_no, new_bill_no, retail_cust_mobile_no, retail_cust_name, retail_cust_email_id, LSTITEMS, bill_cancelled, bill_date from snitch_db.maplemonk.LOGICERP_GET_SALE_INVOICE where bill_no not in (select distinct bill_no from snitch_db.maplemonk.LOGICERPNEW_GET_SALE_INVOICE)) ; create or replace table snitch_db.maplemonk.unicommerce_fact_items_intermediate as with order_related as ( select replace(saleorderdto:channel,\'\"\',\'\') marketplace, case when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%ajio%\' then \'AJIO\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%myntra%\' then \'MYNTRA\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%fynd%\' then \'FYND\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%amazon%\' then \'AMAZON\' when lower(replace(saleorderdto:channel,\'\"\',\'\')) like \'%flipkart%\' then \'FLIPKART\' else replace(saleorderdto:channel,\'\"\',\'\') end as marketplace_mapped, replace(saleorderdto:source,\'\"\',\'\') as source, replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(saleorderdto:displayOrderCode,\'\"\',\'\') as order_name, replace(saleorderdto:billingAddress:phone,\'\"\',\'\') as phone, replace(saleorderdto:billingAddress:name,\'\"\',\'\') as name, saleorderdto:billingAddress:email::varchar as email, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:updated,\'1970-01-01\'))) as SHIPPING_LAST_UPDATE_DATE, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:updated,\'1970-01-01\')) as SHIPPING_LAST_UPDATE_timestamp, replace(A.Value:itemSku,\'\"\',\'\') as sku, REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, replace(A.Value:channelProductId,\'\"\',\'\') as product_id, replace(A.Value:itemName,\'\"\',\'\') as product_name, replace(saleorderdto:currencyCode,\'\"\',\'\') as currency, replace(saleorderdto:billingAddress:city,\'\"\',\'\') as city, replace(saleorderdto:billingAddress:state,\'\"\',\'\') as state, replace(saleorderdto:billingAddress:country,\'\"\',\'\') as country, replace(saleorderdto:billingAddress:pincode,\'\"\',\'\') as pincode, replace(saleorderdto:status,\'\"\',\'\') as ORDER_STATUS, date(CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\'))) as order_date, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:displayOrderDateTime,\'1970-01-01\')) as order_timestamp, A.Value:shippingCharges::float as shipping_price, A.Value:packetNumber::int as SUBORDER_QUANTITY, A.Value:discount::float as discount, A.Value:totalIntegratedGst::float as tax, A.Value:totalPrice::float as SELLING_PRICE, replace(A.Value:shippingPackageCode,\'\"\',\'\') as shippingPackageCode, replace(A.Value:shippingPackageStatus,\'\"\',\'\') as shippingPackageStatus, replace(A.Value:facilityName,\'\"\',\'\') as warehouse_name, replace(A.Value:code,\'\"\',\'\') as saleOrderItemCode, replace(A.Value:maxRetailPrice,\'\"\',\'\') as MRP, A.Value:id::varchar as SALES_ORDER_ITEM_ID from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:saleOrderItems)A ), shipping_related as ( select replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(A.Value:shippingProvider,\'\"\',\'\') as courier, replace(A.Value:status,\'\"\',\'\') shipping_status, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',A.Value:dispatched,\'1970-01-01\')) dispatched_timestamp, A.Value:dispatched dispatched, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',A.Value:delivered,\'1970-01-01\')) delivered_timestamp, A.Value:delivered delivered, replace(A.Value:trackingNumber,\'\"\',\'\') awb, A.Value:code as shippingPackageCode, to_timestamp_ntz(A.value:invoiceDate::int/1000) as Invoice_date, case when replace(A.Value:status,\'\"\',\'\') = \'RETURNED\' then A.Value:updated end return_date, coalesce(replace(A.value:shippingCourier,\'\"\',\'\'),replace(A.Value:shippingProvider,\'\"\',\'\')) shipping_courier from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A ), order_address as ( select * from ( select replace(saleorderdto:code,\'\"\',\'\') as order_id, replace(A.value:addressLine1,\'\"\',\'\') address_line_1, replace(A.value:addressLine2,\'\"\',\'\') address_line_2, replace(A.value:pincode,\'\"\',\'\') Pincode, replace(A.value:city,\'\"\',\'\') City, replace(A.value:state,\'\"\',\'\') State, replace(A.value:Country,\'\"\',\'\') Country, row_number () over (partition by replace(saleorderdto:code,\'\"\',\'\')order by 1 ) rw from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:addresses)A )where rw = 1 ), sku_shipping_quantity as ( select * from (select replace(saleorderdto:code,\'\"\',\'\') ordeR_id, replace(B.value:itemSku, \'\"\',\'\') sku, replace(B.value:quantity, \'\"\',\'\') shipping_quantity, row_number () over (partition by replace(saleorderdto:code,\'\"\',\'\'), replace(B.value:itemSku, \'\"\',\'\') order by 1 ) rw from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages)A, lateral flatten (input => A.value:items) B) where rw = 1 ), returns as ( select replace(saleorderdto:code,\'\"\',\'\') as order_id, B.Value:saleOrderItemCode as saleOrderItemCode, B.Value:itemSku as itemSku,B.* from snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:returns)A, LATERAL FLATTEN (INPUT => A.Value:returnItems)B ) select o.marketplace, coalesce(marketplace_mapped,c.channel) marketplace_mapped, source, o.order_id, o.order_name, o.phone, o.name, o.email, o.SHIPPING_LAST_UPDATE_DATE, o.SHIPPING_LAST_UPDATE_timestamp, o.sku, o.sku_group, o.product_id, o.product_name, currency, oa.address_line_1, oa.address_line_2, oa.city, oa.state, oa.country, oa.pincode, o.ORDER_STATUS, coalesce(order_date::Date, c.date) ordeR_Date, order_timestamp, shipping_price, SUBORDER_QUANTITY, ssq.shipping_quantity shipping_quantity, discount, tax, SELLING_PRICE, o.shippingPackageCode, o.shippingPackageStatus, warehouse_name, o.saleOrderItemCode, o.MRP, o.SALES_ORDER_ITEM_ID, s.courier, s.shipping_courier, s.shipping_status, ss.created_timestamp, ss.picking_timestamp, ss.picked_timestamp, ss.packed_timestamp, ss.manifested_timestamp, s.dispatched_timestamp, date(s.dispatched) Dispatch_date, date(s.delivered) Delivered_Date, s.delivered_timestamp, date(s.return_date) Return_Date, (dispatch_date - order_date)::int days_to_dispatch, s.awb, case when r.itemSku is not NUll then 1 else 0 end as return_flag, case when return_flag = 1 then suborder_quantity else 0 end::int as return_quantity, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE( o.product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, case when order_status=\'COMPLETE\' then delivered_date-order_date else current_date - order_date end as days_in_shipment, case when order_status=\'COMPLETE\' then delivered_date-dispatch_Date else current_date - order_date end as dispatch_to_delivery_days, s.invoice_date, d.cost, coalesce(CASE WHEN e.product_type = \'\' THEN \'NA\' WHEN e.product_type = \'Jeans\' THEN \'Denim\' WHEN e.product_type = \'Pant\' then \'Pants\' ELSE e.product_type END,d.category) AS category, coalesce(e.product_name, o.product_name) producT_name_shopify, div0(pla_spends, count(*) over (partition by o.marketplace_mapped, o.order_date::date)) pla_spends, div0(banner_spends, count(*) over (partition by o.marketplace_mapped, o.order_date::date)) banner_spends, div0(t.sales_target,count(*) over (partition by date_trunc(\'month\', o.order_date::date), marketplace_mapped)) sales_target, div0(t.spends_target,count(*) over (partition by date_trunc(\'month\', o.order_date::date), marketplace_mapped)) spends_target from order_related o left join shipping_related s on o.shippingPackageCode= s.shippingPackageCode and o.order_id = s.order_id left join sku_shipping_quantity ssq on o.ordeR_id = ssq.ordeR_id and o.sku = ssq.sku left join returns r on r.saleOrderItemCode = o.saleOrderItemCode and r.order_id=o.order_id left join order_address oa on oa.order_id = o.order_id left join ( select distinct \"Sku Code\", \"Cost Price\"::float cost, CATEGORY from snitch_db.maplemonk.unicommerce_cost ) d on d.\"Sku Code\" = o.sku left join ( select * from( select b.value:\"sku\" as SKU, title product_name, product_type, row_number() over (partition by b.value:\"sku\" order by updated_at desc) rw from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS, lateral flatten (INPUT => variants)b ) where rw = 1 ) e on lower(o.sku) = lower(e.sku) full outer join ( select coalesce(a.date, b.date) date, coalesce(a.channel, b.channel) channel, pla_spends, banner_spends from (select date::date date, channel, sum(replace(spends,\',\',\'\')::float) pla_spends from snitch_db.maplemonk.mp_marketplace_spends where type = \'PLA\' group by 1,2 order by date desc )a full outer join (select date::date date, channel, sum(replace(spends,\',\',\'\')::float) banner_spends from snitch_db.maplemonk.mp_marketplace_spends where type = \'Banner\' group by 1,2 order by date desc )b on a.date = b.date and a.channel = b.channel ) c on c.date = o.order_date::date and lower(c.channel) = lower(o.marketplace_mapped) left join ( select coalesce (a.date, b.date) date, coalesce (a.marketplace, b.marketplace) marketplace, a.target sales_target, b.target spends_target from (select * from snitch_db.maplemonk.mp_monthly_target_marketplace where metric = \'Sales\') a full outer join (select * from snitch_db.maplemonk.mp_monthly_target_marketplace where metric = \'Spends\') b on a.date = b.date and a.marketplace = b.marketplace ) t on date_trunc(\'month\', o.order_date::date) = t.date and lower(o.marketplace_mapped) = lower(t.marketplace) left join snitch_db.maplemonk.shipping_timestamps ss on o.shippingPackageCode= ss.SHIPPING_PACKAGE_CODE and o.order_id = ss.order_id union all select o.*, CASE WHEN e.product_type = \'\' THEN \'NA\' WHEN e.product_type = \'Jeans\' THEN \'Denim\' WHEN e.product_type = \'Pant\' then \'Pants\' ELSE e.product_type END AS category, coalesce(e.product_name, o.product_name) producT_name_shopify, null pla_spends, null banner_spends, null sales_target, null spends_target from ( select Branch_Name::varchar as marketplace, Branch_Name::varchar as marketplace_mapped, BRANCH_SHORT_NAME::varchar as source, bill_no::varchar as order_id, new_bill_no::varchar as ordeR_name, retail_cust_mobile_no::varchar as phone, retail_cust_name::varchar as name, retail_cust_email_id::varchar as email, null as SHIPPING_LAST_UPDATE_DATE, null as SHIPPING_LAST_UPDATE_timestamp, replace(A.Value:AddlItemCode,\'\"\',\'\')::varchar as sku, REVERSE(SUBSTRING(REVERSE(sku::varchar), CHARINDEX(\'-\', REVERSE(sku::varchar)) + 1)) AS sku_group, replace(A.Value:HSN_Code::varchar,\'\"\',\'\') as product_id, null as product_name, \'INR\' as currency, null as address_line1, null as address_line2, null as city, null as state, null as country, null as pincode, case when lower(bill_cancelled) = \'false\' then \'Processed\' else \'Cancelled\' end as ORDER_STATUS, to_date(bill_date, \'DD/MM/YYYY\') as order_date, null as order_timestamp, null as shipping_price, A.Value:Quantity::int as SUBORDER_QUANTITY, A.Value:Quantity::int as shipping_quantity, (A.Value:CD::float)*-1 as discount, (A.Value:Tax_Amt_1::float) + (A.Value:Tax_Amt_3::float) as tax, A.Value:Net_Amt::varchar as SELLING_PRICE, null as shippingPackageCode, null as shippingPackageStatus, replace(A.Value:Godown_Name::varchar,\'\"\',\'\') as warehouse_name, replace(A.Value:SO_Item_Order_ID::varchar,\'\"\',\'\') as saleOrderItemCode, replace(A.Value:Item_MRP::varchar,\'\"\',\'\') as MRP, replace(A.Value:SO_Item_Order_ID::varchar,\'\"\',\'\') as SALES_ORDER_ITEM_ID, null as courier, null as shipping_courier, null as shipping_status, null as created_timestamp, null as picking_timestamp, null as picked_timestamp, null as packed_timestamp, null as manifested_timestamp, null as dispatched_timestamp, to_date(bill_date, \'DD/MM/YYYY\') Dispatch_date, to_date(bill_date, \'DD/MM/YYYY\') Delivered_Date, null as delivered_timestamp, null Return_Date, 0 as days_to_dispatch, null as awb, null as return_flag, null as return_quantity, case when order_status = \'CANCELLED\' then suborder_quantity else 0 end::int as cancelled_quantity, case when row_number()over(partition by phone order by order_date asc) = 1 then \'New\' else \'Repeat\' end as new_customer_flag, FIRST_VALUE(product_name) OVER ( PARTITION BY phone ORDER BY order_date asc ) AS acquisition_product, 0 as days_in_shipment, 0 as dispatch_to_delivery_days, null as invoice_date, null as cost from snitch_db.maplemonk.LOGICERPConsolidated_GET_SALE_INVOICE, LATERAL FLATTEN (INPUT => LSTITEMS)A) o left join ( select * from ( select replace(b.value:\"sku\",\'\"\',\'\') as SKU, title product_name, product_type, row_number() over (partition by sku order by product_name) rw from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS, lateral flatten (INPUT => variants)b ) where rw = 1 ) e on lower(o.sku) = lower(e.sku) ; create or replace table snitch_db.maplemonk.pincode_precentiles as select pincode, marketplace_mapped, shipping_courier, dispatch_to_delivery_days, orders, share, 50 as percentile from ( select *, row_number() over (partition by pincode, marketplace_mapped, shipping_courier order by share) rw from (select *, div0(SUM(orders) OVER (PARTITION BY pincode, shipping_courier,marketplace_mapped ORDER BY dispatch_to_delivery_days ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), sum(orders) over (partition by pincode,marketplace_mapped, shipping_courier)) share from (select pincode, dispatch_to_delivery_days,marketplace_mapped, shipping_courier, count(distinct order_id) orders from snitch_db.maplemonk.unicommerce_fact_items_intermediate where dispatch_Date is not null and delivered_Date is not null and order_status <> \'CANCELLED\' group by 1,2,3,4 ) ) where share > 0.5 ) where rw = 1 union all select pincode, marketplace_mapped, shipping_courier, dispatch_to_delivery_days, orders, share, 90 as percentile from ( select *, row_number() over (partition by pincode,marketplace_mapped, shipping_courier order by share) rw from (select *, div0(SUM(orders) OVER (PARTITION BY pincode,marketplace_mapped, shipping_courier ORDER BY dispatch_to_delivery_days ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), sum(orders) over (partition by pincode,marketplace_mapped, shipping_courier)) share from (select pincode, marketplace_mapped,shipping_courier, dispatch_to_delivery_days, count(distinct order_id) orders from snitch_db.maplemonk.unicommerce_fact_items_intermediate where dispatch_Date is not null and delivered_Date is not null and order_status <> \'CANCELLED\' group by 1,2,3,4 ) ) where share > 0.9 ) where rw = 1 ; create or replace table snitch_db.maplemonk.customerID_test as with new_phone_numbers as ( select contact_num ,9700000000 + row_number() over( order by contact_num asc ) as maple_monk_id from ( select distinct right(regexp_replace(phone, \'[^a-zA-Z0-9]+\'),10) as contact_num from snitch_db.maplemonk.unicommerce_fact_items_intermediate ) a ), int as ( select contact_num,email,coalesce(maple_monk_id,id2) as maple_monk_id from ( select contact_num, email,maple_monk_id,9800000000+row_number() over(partition by maple_monk_id is NULL order by email asc ) as id2 from ( select distinct coalesce(p.contact_num,right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10)) as contact_num, e.email,maple_monk_id from ( select phone as contact_num,email from snitch_db.maplemonk.unicommerce_fact_items_intermediate ) e left join new_phone_numbers p on p.contact_num = right(regexp_replace(e.contact_num, \'[^a-zA-Z0-9]+\'),10) ) a ) b ) select contact_num,email,case when email is not null and email <> \'\' then min(maple_monk_id) over (partition by email ) else maple_monk_id end maple_monk_id from int where coalesce(contact_num,email) is not NULL; create or replace table snitch_db.maplemonk.unicommerce_fact_items_snitch as select distinct coalesce(c.maple_monk_id,c.maple_monk_id) customer_id, o.*, min(order_date) over(partition by customer_id) as acquisition_date, d.dispatch_to_delivery_days dispatch_to_delivery_days_50th_percentile, e.dispatch_to_delivery_days dispatch_to_delivery_days_90th_percentile, AM.SKU_CLASS from snitch_db.maplemonk.unicommerce_fact_items_intermediate o left join (select distinct contact_num phone,maple_monk_id from snitch_db.maplemonk.customerID_test )c on c.phone = o.phone left join snitch_db.maplemonk.pincode_precentiles d on o.pincode = d.pincode and o.marketplace_mapped = d.marketplace_mapped and o.shipping_courier = d.shipping_courier and d.percentile = 50 left join snitch_db.maplemonk.pincode_precentiles e on o.pincode = e.pincode and o.marketplace_mapped = e.marketplace_mapped and o.shipping_courier = e.shipping_courier and e.percentile = 90 left join (select * from (select *,row_number() over(partition by sku_group order by 1)rw from snitch_db.maplemonk.availability_master) where rw=1) AM on AM.sku_group = o.sku_group ; create or replace table snitch_db.maplemonk.best_courier_by_pincode as select a.pincode, best_shipping_courier, \"50th\", \"90th\",b.orders from (select pincode, best_shipping_courier, \"50th\", \"90th\" from ( select *, row_number() over (partition by pincode order by \"50th\" asc, \"90th\" asc) rw from ( select pincode, shipping_courier best_shipping_courier, max(dispatch_to_delivery_days_50th_percentile) as \"50th\", max(dispatch_to_delivery_days_90th_percentile) \"90th\", count(distinct order_id) orders from ( snitch_db.maplemonk.unicommerce_fact_items_snitch ) where lower(marketplace_mapped) like \'%shopify%\' group by 1,2 )where orders>20)where rw = 1)a left join (select pincode, count(distinct order_id) orders from snitch_db.maplemonk.unicommerce_fact_items_snitch where lower(marketplace_mapped) like \'%shopify%\' group by 1) b on a.pincode = b.pincode ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        