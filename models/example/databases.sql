{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table ALMOWEAR_DB.maplemonk.sales_consolidated_AW as with a as ( select order_date, sku, max(suborder_mrp) as mrp from ALMOWEAR_DB.maplemonk.easy_ecom_consolidated_AW group by 1,2 ) select CUSTOMER_ID, \'Almo\' as SHOP_NAME, NULL as carrier_id, NULL as courier, NULL as email, NULL as contact_num, case when shop_name = \'Amazon\' then \'Amazon.in\' else \'Shopify_India\' end as MARKETPLACE, NULL as MARKETPLACE_ID, ORDER_ID, ORDER_ID as Invoice_id, NUll as shipping_last_update_date, order_status as shipping_status, b.SKU, NULL as sku_type, PRODUCT_ID, PRODUCT_NAME as PRODUCTNAME, CURRENCY, IS_REFUND, CITY::varchar City, STATE:: varchar State, order_status, date(ORDER_timestamp) as ORDER_Date, shipping_price::float as SHIPPING_PRICE, NULL as number_of_products_in_combo, quantity::int suborder_quantity, quantity::int shipped_quantity, case when is_refund = 1 then quantity::int end returned_quantity, case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity, case when is_refund = 1 then line_item_sales end as return_sales, case when is_refund = 0 and lower(order_status) in (\'cancelled\') then line_item_sales end as cancel_sales, TAX::float Tax, a.mrp as suborder_mrp, b.mrp as product_mrp, range, category, style, collection, discount::float as discount , case when line_item_sales::float is null then 0 else line_item_sales::float end selling_price, coalesce(a.mrp,b.mrp)*suborder_quantity as mrp_sales, case when new_customer_flag = \'New\' then 1 else 0 end as new_customer_flag, NULL as Days_in_Shipment from almowear_db.maplemonk.FACT_ITEMS b left join a on a.order_date = b.order_timestamp::date and a.sku= b.sku where lower(shop_name) not like \'%hop%\' union all select NULL as customer_id, * from ALMOWEAR_DB.maplemonk.easy_ecom_consolidated_AW ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ALMOWEAR_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        