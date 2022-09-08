{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table MM_TEST.NaveenSept7.MM_TEST_EasyEcom_FACT_ITEMS as with orders as ( select * from ( select *,row_number()over(partition by order_id,invoice_id order by last_update_date desc) as rw from MM_TEST.NaveenSept7.LilGoodness_Test_CUSTOMER_ORDERS c ) a where rw =1 ), first_order as ( select * from ( select *,row_number()over(partition by contact_num order by order_date asc) as rw from MM_TEST.NaveenSept7.LilGoodness_Test_CUSTOMER_ORDERS c ) a where rw =1 ) select Marketplace as SHOP_NAME, carrier_id, courier, customer_name, email, contact_num, MARKETPLACE, MARKETPLACE_ID, ORDER_ID, Suborder_id, invoice_id, reference_code, manifest_date, shipping_Last_update_date shipping_last_update_date, shipping_status, replace(s.SKU, \'\"\',\'\') SKU, replace(sku_type, \'\"\',\'\') sku_type, PRODUCT_ID, replace(PRODUCTNAME, \'\"\',\'\') PRODUCTNAME, CURRENCY, IS_REFUND, City, State, order_status, ORDER_Date, SHIPPING_PRICE, number_of_products_in_combo, suborder_quantity, shipped_quantity, returned_quantity, cancelled_quantity, return_sales, cancel_sales, Tax, suborder_mrp, replace(category, \'\"\',\'\') category, discount, selling_price, coalesce(suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<selling_price) then discount else mrp_sales-selling_price end Discount_MRP, new_customer_flag, new_customer_flag_month, Warehouse_Name, Days_in_Shipment, Channel, payment_mode, import_date, last_update_date from ( select o.MARKETPLACE as SHOP_NAME, o.carrier_id, o.courier, o.customer_name, o.email, o.contact_num, o.MARKETPLACE, o.MARKETPLACE_ID, o.ORDER_ID, A.Value:suborder_id Suborder_id, o.invoice_id, o.reference_code, o.manifest_date, o.shipping_Last_update_date shipping_last_update_date, o.shipping_status, A.Value:marketplace_sku as sku, A.Value:sku_type sku_type, A.Value:product_id PRODUCT_ID, A.Value:productName PRODUCTNAME, case when o.pickup_country=\'India\' then \'INR\' end as CURRENCY, case when A.Value:returned_quantity::int >0 then 1 else 0 end as IS_REFUND, upper(o.CITY::varchar) City, upper(o.STATE::varchar) State, o.order_status, o.ORDER_Date as ORDER_Date, ifnull(A.Value:total_shipping_charge::float,0) as SHIPPING_PRICE, A.Value:item_quantity::int as number_of_products_in_combo, A.Value:suborder_quantity::int suborder_quantity, A.Value:shipped_quantity::int shipped_quantity, A.Value:returned_quantity::int returned_quantity, A.Value:cancelled_quantity::int cancelled_quantity, case when replace(A.Value:selling_price,\',\',\'\')=\'#N/A\' then A.Value:mrp::float when replace(A.Value:selling_price,\',\',\'\') is null then 0 else replace(A.Value:selling_price,\',\',\'\')::float end selling_price, case when returned_quantity > 0 then selling_price*returned_quantity/A.Value:item_quantity end as return_sales, case when cancelled_quantity > 0 then selling_price end cancel_sales, ifnull(A.Value:TAX::float,0) Tax, A.Value:mrp::float as suborder_mrp, A.Value:category category, -1*(coalesce(o.total_discount*(case when selling_price=0 then 1 else selling_price end/case when sum(selling_price)over (partition by o.order_id) = 0 then 1 else sum(selling_price)over (partition by o.order_id) end ),0)::float) as discount , case when fo.order_id = o.order_id then \'New\' else \'Repeat\' end as new_customer_flag, case when date_trunc(\'month\',fo.order_date::date) = date_trunc(\'month\',o.order_date::date) then \'New\' else \'Repeat\' end as new_customer_flag_month, o.import_warehouse_name as Warehouse_Name, case when o.shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(o.ORDER_Date), getdate()) when o.shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(o.ORDER_Date),date(o.shipping_Last_update_date)) end::int as Days_in_Shipment, o.marketplace as Channel, o.payment_mode, o.import_date, o.last_update_date from orders o left join first_order fo on fo.contact_num = o.contact_num ,LATERAL FLATTEN (INPUT => o.SUBORDERS)A ) s;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MM_TEST.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        