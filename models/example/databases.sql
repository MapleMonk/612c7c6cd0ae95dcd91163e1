{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SELECT_DB.MAPLEMONK.SELECT_DB_EasyEcom_FACT_ITEMS as with orders as ( select * from ( select *,row_number()over(partition by order_id,invoice_id order by last_update_date desc) as rw from SELECT_DB.MAPLEMONK.EasyEcom_easyecom_select_CUSTOMER_ORDERS c ) a where rw =1 ), first_order as ( select * from ( select *,row_number()over(partition by contact_num order by order_date asc) as rw from SELECT_DB.MAPLEMONK.EasyEcom_easyecom_select_CUSTOMER_ORDERS c ) a where rw =1 ), pick_up_dates as ( select * from (select o.ORDER_ID, A.Value:suborder_id Suborder_id, o.invoice_id, o.reference_code, o.shipping_history, B.value:time::timestamp picked_up_time ,row_number() over (partition by A.Value:suborder_id order by B.value:time::timestamp) rw from orders o ,LATERAL FLATTEN (INPUT => o.SUBORDERS) A, LATERAL FLATTEN (INPUT => parse_json(o.shipping_history)) B where lower(B.value:status) like any (\'picked up\', \'pickupdone\') ) where rw =1 ), intransit_dates as ( select * from (select o.ORDER_ID, A.Value:suborder_id Suborder_id, o.invoice_id, o.reference_code, o.shipping_history, B.value:time::timestamp intransit_time ,row_number() over (partition by A.Value:suborder_id order by B.value:time::timestamp) rw from orders o ,LATERAL FLATTEN (INPUT => o.SUBORDERS) A, LATERAL FLATTEN (INPUT => parse_json(o.shipping_history)) B where lower(B.value:status) like any (\'in transit\', \'%arrivedat%\') ) where rw = 1 ), delivery_dates as ( select * from (select o.ORDER_ID, A.Value:suborder_id Suborder_id, o.invoice_id, o.reference_code, o.shipping_history, B.value:time::timestamp delivered_time ,row_number() over (partition by A.Value:suborder_id order by B.value:time::timestamp) rw from orders o ,LATERAL FLATTEN (INPUT => o.SUBORDERS) A, LATERAL FLATTEN (INPUT => parse_json(o.shipping_history)) B where lower(B.value:status) like \'delivered\' ) where rw = 1 ) select upper(Marketplace) as SHOP_NAME, carrier_id, upper(courier) as courier, upper(customer_name) customer_name, email, contact_num, upper(MARKETPLACE) marketplace, MARKETPLACE_ID, s.ORDER_ID, s.Suborder_id, s.invoice_id, s.reference_code, Marketplace_LineItem_ID, manifest_date, shipping_Last_update_date shipping_last_update_date, coalesce(dd.delivered_time,case when lower(shipping_status) = \'delivered\' then shipping_last_update_date end) as Delivered_Date, upper(coalesce(shipping_status,case when lower(queue_message) like \'%error%\' then \'SHIPMENT CREATION ERROR\' end)) shipping_status, awb, replace(s.SKU, \'\"\',\'\') SKU, replace(s.easy_ecom_sku, \'\"\',\'\')easy_ecom_sku, upper(replace(sku_type, \'\"\',\'\')) sku_type, PRODUCT_ID, upper(replace(PRODUCTNAME, \'\"\',\'\')) PRODUCTNAME, CURRENCY, IS_REFUND, upper(City) City, upper(State) State, upper(order_status) order_Status, ORDER_Date, SHIPPING_PRICE, number_of_products_in_combo, suborder_quantity, shipped_quantity, returned_quantity, cancelled_quantity, return_sales, cancel_sales, Tax, suborder_mrp, upper(replace(s.category, \'\"\',\'\')) category, discount, selling_price, coalesce(suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<selling_price) then discount else mrp_sales-selling_price end Discount_MRP, new_customer_flag, new_customer_flag_month, upper(Warehouse_Name) Warehouse_name, Days_in_Shipment, upper(Channel) Channel, upper(payment_mode) Payment_mode, import_date, last_update_date, pd.picked_up_time, id.intransit_time, dd.delivered_time, upper(p.name) as mapped_product_name, upper(p.category) as mapped_Category, upper(p.sub_category) as mapped_sub_category, queue_message from ( select o.MARKETPLACE as SHOP_NAME, o.carrier_id, o.courier, o.customer_name, o.email, o.contact_num, o.MARKETPLACE, o.MARKETPLACE_ID, o.ORDER_ID, A.Value:suborder_id Suborder_id, o.invoice_id, o.reference_code, o.manifest_date, o.shipping_Last_update_date shipping_last_update_date, o.shipping_status, o.awb_number awb, A.Value:marketplace_sku as sku, A.Value:sku as easy_ecom_sku, A.Value:sku_type sku_type, A.Value:product_id PRODUCT_ID, A.Value:suborder_num Marketplace_LineItem_ID, A.Value:productName PRODUCTNAME, case when o.pickup_country=\'India\' then \'INR\' end as CURRENCY, case when A.Value:returned_quantity::int >0 then 1 else 0 end as IS_REFUND, upper(o.CITY::varchar) City, upper(o.STATE::varchar) State, o.order_status, o.ORDER_Date as ORDER_Date, ifnull(A.Value:total_shipping_charge::float,0) as SHIPPING_PRICE, A.Value:item_quantity::int as number_of_products_in_combo, A.Value:suborder_quantity::int suborder_quantity, A.Value:shipped_quantity::int shipped_quantity, A.Value:returned_quantity::int returned_quantity, A.Value:cancelled_quantity::int cancelled_quantity, case when typeof(A.Value:selling_price)=\'VARCHAR\' then A.Value:mrp::float when replace(A.Value:selling_price,\',\',\'\') is null then 0 else replace(A.Value:selling_price,\',\',\'\')::float end selling_price, case when returned_quantity > 0 then selling_price*returned_quantity/A.Value:item_quantity end as return_sales, case when cancelled_quantity > 0 then selling_price end cancel_sales, ifnull(A.Value:TAX::float,0) Tax, A.Value:mrp::float as suborder_mrp, A.Value:category category, -1*(coalesce(o.total_discount*(case when selling_price=0 then 1 else selling_price end/case when sum(selling_price)over (partition by o.order_id) = 0 then 1 else sum(selling_price)over (partition by o.order_id) end ),0)::float) as discount , case when fo.order_id = o.order_id then \'New\' else \'Repeat\' end as new_customer_flag, case when date_trunc(\'month\',fo.order_date::date) = date_trunc(\'month\',o.order_date::date) then \'New\' else \'Repeat\' end as new_customer_flag_month, o.import_warehouse_name as Warehouse_Name, case when o.shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(o.ORDER_Date), getdate()) when o.shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(o.ORDER_Date),date(o.shipping_Last_update_date)) end::int as Days_in_Shipment, o.marketplace as Channel, o.payment_mode, o.import_date, o.queue_message, o.last_update_date from orders o left join first_order fo on fo.contact_num = o.contact_num ,LATERAL FLATTEN (INPUT => o.SUBORDERS) A ) s left join pick_up_dates pd on pd.Suborder_id = s.suborder_id left join intransit_dates id on id.Suborder_id = s.suborder_id left join delivery_dates dd on dd.Suborder_id = s.suborder_id left join (select * from (select skucode, name, category, sub_category, row_number() over (partition by skucode order by 1) rw from SELECT_DB.MAPLEMONK.sku_master) where rw = 1 ) p on s.sku = p.skucode; create or replace table SELECT_DB.MAPLEMONK.SELECT_DB_easyecom_returns_intermediate as select ORDER_ID ,INVOICE_ID ,RI.VALUE:\"suborder_id\" SUBORDER_ID ,REFERENCE_CODE ,CREDIT_NOTE_ID ,try_to_timestamp(ORDER_DATE) ORDER_DATE ,try_to_timestamp(INVOICE_DATE) INVOICE_DATE ,try_to_timestamp(RETURN_DATE) RETURN_DATE ,try_to_timestamp(MANIFEST_DATE) MANIFEST_DATE ,try_to_timestamp(IMPORT_DATE) IMPORT_DATE ,try_to_timestamp(LAST_UPDATE_DATE) LAST_UPDATE_DATE ,RI.VALUE:company_product_id COMPANY_PRODUCT_ID ,replace(RI.VALUE:productName,\'\"\',\'\') PRODUCTNAME ,RI.VALUE:product_id PRODUCT_ID ,replace(RI.VALUE:sku,\'\"\',\'\') SKU ,MARKETPLACE ,MARKETPLACE_ID ,REPLACEMENT_ORDER ,replace(RI.VALUE:return_reason,\'\"\',\'\') RETURN_REASON ,ifnull(RI.VALUE:returned_item_quantity::float,0) RETURNED_QUANTITY ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) RETURN_AMOUNT_WITHOUT_TAX ,ifnull(RI.Value:credit_note_total_item_tax::float,0) RETURN_TAX ,ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0) RETURN_SHIPPING_CHARGE ,ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) RETURN_MISC ,ifnull(RI.Value:credit_note_total_item_excluding_tax::float,0) + ifnull(RI.Value:credit_note_total_item_tax::float,0) + ifnull(RI.Value:credit_note_total_item_shipping_charge::float,0)+ifnull(RI.VALUE:credit_note_total_item_miscellaneous::float,0) TOTAL_RETURN_AMOUNT from SELECT_DB.MAPLEMONK.EasyEcom_easyecom_select_RETURNS R, LATERAL flatten(INPUT => R.ITEMS) RI;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        