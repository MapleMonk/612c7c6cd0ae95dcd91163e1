{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table GLADFUL_DB.MAPLEMONK.GLADFUL_EasyEcom_FACT_ITEMS as with orders as ( select * from ( select *,row_number()over(partition by order_id,invoice_id order by last_update_date desc) as rw from GLADFUL_DB.MAPLEMONK.GLADFUL_EASYECOM_CUSTOMER_ORDERS c ) a where rw =1 ), first_order as ( select * from ( select *,row_number()over(partition by contact_num order by order_date asc) as rw from GLADFUL_DB.MAPLEMONK.GLADFUL_EASYECOM_CUSTOMER_ORDERS c ) a where rw =1 ) select case when upper(s.Marketplace) = \'B2B\' then upper(coalesce(B2B.mapped_B2B_name,s.marketplace)) else upper(s.Marketplace) end as SHOP_NAME, carrier_id, upper(courier) as courier, upper(s.customer_name) customer_name, email, contact_num, upper(s.MARKETPLACE) marketplace, MARKETPLACE_ID, ORDER_ID, Suborder_id, invoice_id, reference_code, Marketplace_LineItem_ID, manifest_date, shipping_Last_update_date shipping_last_update_date, case when lower(shipping_status) = \'delivered\' then shipping_last_update_date end as Delivered_Date, upper(shipping_status) shipping_status, awb, replace(s.SKU, \'\"\',\'\') SKU, upper(replace(sku_type, \'\"\',\'\')) sku_type, PRODUCT_ID, upper(replace(PRODUCTNAME, \'\"\',\'\')) PRODUCTNAME, CURRENCY, IS_REFUND, upper(City) City, upper(State) State, upper(order_status) order_Status, ORDER_Date, SHIPPING_PRICE, number_of_products_in_combo, suborder_quantity, shipped_quantity, returned_quantity, cancelled_quantity, return_sales, cancel_sales, Tax, suborder_mrp, upper(replace(s.category, \'\"\',\'\')) category, discount, selling_price, coalesce(suborder_mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<selling_price) then discount else mrp_sales-selling_price end Discount_MRP, new_customer_flag, new_customer_flag_month, upper(Warehouse_Name) Warehouse_name, Days_in_Shipment, upper(Channel) Channel, upper(payment_mode) Payment_mode, import_date, last_update_date, p.skucode SKU_CODE, upper(coalesce(p.name,replace(PRODUCTNAME, \'\"\',\'\'))) as mapped_product_name, upper(coalesce(p.category,replace(s.category, \'\"\',\'\'))) as mapped_Category, upper(p.sub_category) as mapped_sub_category from ( select o.MARKETPLACE as SHOP_NAME, o.carrier_id, o.courier, o.customer_name, o.email, o.contact_num, o.MARKETPLACE, o.MARKETPLACE_ID, o.ORDER_ID, A.Value:suborder_id Suborder_id, o.invoice_id, o.reference_code, o.manifest_date, o.shipping_Last_update_date shipping_last_update_date, o.shipping_status, o.awb_number awb, A.Value:marketplace_sku as sku, A.Value:sku_type sku_type, A.Value:product_id PRODUCT_ID, A.Value:suborder_num Marketplace_LineItem_ID, A.Value:productName PRODUCTNAME, case when o.pickup_country=\'India\' then \'INR\' end as CURRENCY, case when A.Value:returned_quantity::int >0 then 1 else 0 end as IS_REFUND, upper(o.CITY::varchar) City, upper(o.STATE::varchar) State, o.order_status, o.ORDER_Date as ORDER_Date, ifnull(A.Value:total_shipping_charge::float,0) as SHIPPING_PRICE, A.Value:item_quantity::int as number_of_products_in_combo, A.Value:suborder_quantity::int suborder_quantity, A.Value:shipped_quantity::int shipped_quantity, A.Value:returned_quantity::int returned_quantity, A.Value:cancelled_quantity::int cancelled_quantity, ifnull(replace(A.Value:selling_price,\',\',\'\')::float,0) selling_price, case when returned_quantity > 0 then selling_price*returned_quantity/A.Value:item_quantity end as return_sales, case when cancelled_quantity > 0 then selling_price end cancel_sales, ifnull(A.Value:TAX::float,0) Tax, A.Value:mrp::float as suborder_mrp, A.Value:category category, -1*(coalesce(o.total_discount*(case when selling_price=0 then 1 else selling_price end/case when sum(selling_price)over (partition by o.order_id) = 0 then 1 else sum(selling_price)over (partition by o.order_id) end ),0)::float) as discount , case when fo.order_id = o.order_id then \'New\' else \'Repeat\' end as new_customer_flag, case when date_trunc(\'month\',fo.order_date::date) = date_trunc(\'month\',o.order_date::date) then \'New\' else \'Repeat\' end as new_customer_flag_month, o.import_warehouse_name as Warehouse_Name, case when o.shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(o.ORDER_Date), getdate()) when o.shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(o.ORDER_Date),date(o.shipping_Last_update_date)) end::int as Days_in_Shipment, o.marketplace as Channel, o.payment_mode, o.import_date, o.last_update_date from orders o left join first_order fo on fo.contact_num = o.contact_num ,LATERAL FLATTEN (INPUT => o.SUBORDERS)A ) s left join (select * from (select replace(marketplace_product_id,\'`\',\'\') marketplace_skucode, replace(skucode,\'`\',\'\') skucode, productname name, category, sub_category, row_number() over (partition by replace(marketplace_product_id,\'`\',\'\') order by 1) rw from Gladful_db.MAPLEMONK.sku_mapping_master) where rw = 1 ) q on s.sku = q.marketplace_skucode left join (select * from (select replace(skucode,\'`\',\'\') skucode, productname name, category, sub_category, row_number() over (partition by replace(skucode,\'`\',\'\') order by 1) rw from Gladful_db.MAPLEMONK.sku_master) where rw = 1 ) p on q.skucode = p.skucode left join (select * from (select * , row_number() over (partition by lower(customer_name) order by 1) rw from Gladful_DB.MapleMonk.b2b_customer_mapping ) where rw=1 ) B2B on lower(s.customer_name) = lower(B2B.customer_name) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GLADFUL_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        