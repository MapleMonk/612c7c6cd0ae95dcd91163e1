{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table lilgoodness_db.maplemonk.easy_ecom_consolidated as with sub_orders as ( select * from ( select *,row_number()over(partition by suborder_id,sku order by _AIRBYTE_NORMALIZED_AT desc) as rw from EASY_ECOM_CUSTOMER_ORDERS_SUBORDERS c ) a where rw =1 ), first_order as ( select * from ( select *,row_number()over(partition by contact_num order by order_date asc) as rw from EASY_ECOM_CUSTOMER_ORDERS c ) a where rw =1 ) select \'Lil Goodness\' as SHOP_NAME, o.carrier_id, o.courier, o.MARKETPLACE, o.MARKETPLACE_ID, o.ORDER_ID, date(o.shipping_Last_update_date) shipping_last_update_date, o.shipping_status, s.SKU, s.sku_type, s.PRODUCT_ID, s.PRODUCTNAME, case when o.pickup_country=\'India\' then \'INR\' end as CURRENCY, case when returned_quantity >0 then 1 else 0 end as IS_REFUND, o.CITY::varchar City, o.STATE:: varchar State, s.CATEGORY::varchar Category, o.order_status, date(o.ORDER_Date) as ORDER_Date, S.total_shipping_charge::float as SHIPPING_PRICE, s.item_quantity::int as number_of_products_in_combo, s.suborder_quantity::int suborder_quantity, s.shipped_quantity::int shipped_quantity, s.returned_quantity::int returned_quantity, s.cancelled_quantity::int cancelled_quantity, s.TAX::float Tax, coalesce(o.total_discount*(case when selling_price=0 then 1 else selling_price end/case when sum(selling_price)over (partition by o.order_id) = 0 then 1 else sum(selling_price)over (partition by o.order_id) end ),0)::float as discount , case when selling_price::float is null then 0 else selling_price::float end selling_price, case when fo.order_id = o.order_id then 1 else 0 end as new_customer_flag, case when o.shipping_STATUS in (\'In Transit\', \'Shipment Created\') then datediff(day,date(o.ORDER_Date), getdate()) when o.shipping_STATUS in (\'Delivered\',\'Delivered To Origin\') then datediff(day,date(o.ORDER_Date),date(o.shipping_Last_update_date)) end::int as Days_in_Shipment from EASY_ECOM_CUSTOMER_ORDERS o join sub_orders s on o._airbyte_easy_ecom_customer_orders_hashid = s._airbyte_easy_ecom_customer_orders_hashid left join first_order fo on fo.contact_num = o.contact_num",
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
                        