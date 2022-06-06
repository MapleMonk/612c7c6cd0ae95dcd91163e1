{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table LILGOODNESS_DB.maplemonk.fact_items_easy_ecom_LG as with sub_orders as ( select * from ( select *,row_number()over(partition by suborder_id,sku order by _AIRBYTE_NORMALIZED_AT desc) as rw from LILGOODNESS_DB.MAPLEMONK.EASY_ECOM_CUSTOMER_ORDERS_SUBORDERS c ) a where rw =1 ), first_order as ( select * from ( select *,row_number()over(partition by contact_num order by order_date asc) as rw from EASY_ECOM_CUSTOMER_ORDERS c ) a where rw =1 ) select \'LilGoodness\' as SHOP_NAME, o.carrier_id, o.courier, o.customer_name, o.email, o.contact_num, o.MARKETPLACE, o.MARKETPLACE_ID, o.ORDER_ID, o.invoice_id, date(o.shipping_Last_update_date) shipping_last_update_date, o.shipping_status, s.SKU, s.sku_type, s.PRODUCT_ID, s.PRODUCTNAME, case when o.pickup_country=\'India\' then \'INR\' end as CURRENCY, case when returned_quantity >0 then 1 else 0 end as IS_REFUND, o.CITY::varchar City, o.STATE:: varchar State, o.order_status, date(o.ORDER_Date) as ORDER_Date, ifnull(S.total_shipping_charge::float,0) as SHIPPING_PRICE, s.suborder_quantity::int suborder_quantity, s.shipped_quantity::int shipped_quantity, s.returned_quantity::int returned_quantity, s.cancelled_quantity::int cancelled_quantity, case when returned_quantity > 0 then selling_price*returned_quantity/item_quantity end as return_sales, case when cancelled_quantity > 0 then selling_price end cancel_sales, ifnull(s.TAX::float,0) Tax, s.mrp as suborder_mrp, s.category, -1*(coalesce(o.total_discount*(case when selling_price=0 then 1 else selling_price end/case when sum(selling_price)over (partition by o.order_id) = 0 then 1 else sum(selling_price)over (partition by o.order_id) end ),0)::float) as discount , case when selling_price::float is null then 0 else selling_price::float end selling_price, coalesce(s.mrp,0)*suborder_quantity as mrp_sales, case when (mrp_sales is null or mrp_sales<selling_price) then discount else mrp_sales-selling_price end Discount_MRP, case when fo.order_id = o.order_id then 1 else 0 end as new_customer_flag, case when o.shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(o.ORDER_Date), getdate()) when o.shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(o.ORDER_Date),date(o.shipping_Last_update_date)) end::int as Days_in_Shipment, o.MARKETPLACE as Channel from EASY_ECOM_CUSTOMER_ORDERS o join sub_orders s on o._AIRBYTE_EASY_ECOM_CUSTOMER_ORDERS_HASHID = s._AIRBYTE_EASY_ECOM_CUSTOMER_ORDERS_HASHID left join first_order fo on fo.contact_num = o.contact_num;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        