{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hilodesign_db.maplemonk.easy_ecom_consolidated_hilo as with orders as ( select * from ( select *,row_number()over(partition by order_id,invoice_id order by last_update_date desc) as rw from hilodesign_db.MAPLEMONK.easyecom_customer_orders c ) a where rw =1 ), first_order as ( select * from ( select *,row_number()over(partition by contact_num order by order_date asc) as rw from hilodesign_db.maplemonk.easyecom_customer_orders c ) a where rw =1 ) select \'HILO\' as SHOP_NAME, carrier_id, courier, email, contact_num, MARKETPLACE, MARKETPLACE_ID, Channel, ORDER_ID, invoice_id, reference_code, manifest_date, shipping_Last_update_date as shipping_last_update_date, shipping_status, replace(s.SKU, \'\"\',\'\') as SKU, replace(sku_type, \'\"\',\'\') as sku_type, PRODUCT_ID, replace(PRODUCTNAME, \'\"\',\'\') as PRODUCTNAME, CURRENCY, IS_REFUND, City, State, PIN_CODE, order_status, ORDER_Date, SHIPPING_PRICE, number_of_products_in_combo, suborder_quantity, shipped_quantity, returned_quantity, cancelled_quantity, return_sales, replace(cancel_sales,\'\"\',\'\')as cancel_sales, Tax, suborder_mrp, replace(category, \'\"\',\'\') category, discount, selling_price, coalesce(suborder_mrp*suborder_quantity,selling_price) as mrp_sales, case when (mrp_sales is null or mrp_sales<selling_price) then discount else mrp_sales-selling_price end Discount_MRP, new_customer_flag, Warehouse_Name, Days_in_Shipment, payment_mode, import_date, last_update_date, AWB_Number, payment_mode as payment_gateway, cost cogs, case when returned_quantity > 0 then cost end as return_cogs from ( select \'HILO\' as SHOP_NAME, o.carrier_id, o.courier, o.email, o.contact_num, o.MARKETPLACE, o.MARKETPLACE_ID, case when o.marketplace = \'B2B\' and lower(o.email) like \'%aza%\' then \'Aza\' when o.marketplace = \'B2B\' and lower(o.email) like \'%birla%\' then \'Jaypore\' when o.marketplace = \'B2B\' and lower(o.email) like \'%talasha%\' then \'Talasha\' when lower(o.customer_name) like \'%trendia%\' then \'Trendia\' when o.marketplace = \'Offline\' then \'Offline\' when o.marketplace = \'Shopify\' then \'Shopify\' end as Channel, o.ORDER_ID, o.invoice_id, o.reference_code, o.manifest_date, o.shipping_Last_update_date shipping_last_update_date, o.shipping_status, A.Value:sku as sku, A.Value:sku_type sku_type, A.Value:product_id PRODUCT_ID, A.Value:productName PRODUCTNAME, case when o.pickup_country=\'India\' then \'INR\' end as CURRENCY, case when A.Value:returned_quantity >0 then 1 else 0 end as IS_REFUND, o.CITY::varchar City, o.STATE:: varchar State, o.PIN_CODE, o.order_status, o.ORDER_Date as ORDER_Date, ifnull(A.Value:total_shipping_charge::float,0) as SHIPPING_PRICE, A.Value:item_quantity::int as number_of_products_in_combo, A.Value:suborder_quantity::int suborder_quantity, A.Value:shipped_quantity::int shipped_quantity, A.Value:returned_quantity::int returned_quantity, A.Value:cancelled_quantity::int cancelled_quantity, case when returned_quantity > 0 then A.Value:selling_price*returned_quantity/A.Value:item_quantity end as return_sales, case when cancelled_quantity > 0 then A.Value:selling_price end cancel_sales, ifnull(A.Value:TAX::float,0) Tax, A.Value:mrp as suborder_mrp, A.Value:category category, A.value:cost cost, -1*(coalesce(o.total_discount*(case when A.Value:selling_price=0 then 1 else A.Value:selling_price end/case when sum(A.Value:selling_price)over (partition by o.order_id) = 0 then 1 else sum(A.Value:selling_price)over (partition by o.order_id) end ),0)::float) as discount , case when A.Value:selling_price::float is null then 0 else A.Value:selling_price::float end selling_price, case when fo.order_id = o.order_id then 1 else 0 end as new_customer_flag, o.import_warehouse_name as Warehouse_Name, case when o.shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(o.ORDER_Date), getdate()) when o.shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(o.ORDER_Date),date(o.shipping_Last_update_date)) end::int as Days_in_Shipment, o.payment_mode, o.import_date, o.last_update_date , o.AWB_Number from orders o left join first_order fo on fo.contact_num = o.contact_num ,LATERAL FLATTEN (INPUT => o.SUBORDERS)A ) s ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HILODESIGN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        