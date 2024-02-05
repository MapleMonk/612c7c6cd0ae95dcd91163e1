{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table avorganics_db.maplemonk.flipkart_fact_items as select null as CUSTOMER_ID_FINAL ,null as ACQUISITION_DATE ,null as FIRST_COMPLETE_ORDER_DATE ,null as MAPLE_MONK_ID_PHONE ,null as CUSTOMER_ID ,\'Flipkart\' as SHOP_NAME ,\'Flipkart\' as MARKETPLACE ,\'Flipkart\' as CHANNEL ,\'Flipkart\' as SOURCE ,\"Order ID\"::varchar as ORDER_ID ,\"Order ID\"::varchar as REFERENCE_CODE ,null as PHONE ,null as NAME ,null as EMAIL ,null as SHIPPING_LAST_UPDATE_DATE ,\"SKU\"::varchar as SKU ,\"FSN\"::varchar as PRODUCT_ID ,\"Product Title/Description\"::varchar as PRODUCT_NAME ,null as CURRENCY ,null as CITY ,\"Customer\'s Delivery State\" as STATE ,null as ORDER_STATUS ,left(\"Order Date\",10)::date ORDER_DATE ,\"Item Quantity\"::float as QUANTITY ,null as GROSS_SALES_BEFORE_TAX ,replace(\"Total Discount\",\'-\',\'\')::float as DISCOUNT ,null as TAX ,\"Shipping Charges\" as SHIPPING_PRICE ,\"Final Invoice Amount (Price after discount+Shipping Charges)\"::float as SELLING_PRICE ,null as OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null as FINAL_SHIPPING_STATUS ,null as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,null as AWB ,null as PAYMENT_GATEWAY ,null as PAYMENT_MODE ,null as COURIER ,null as DISPATCH_DATE ,left(\"Order Date\",10)::date DELIVERED_DATE ,null as DELIVERED_STATUS ,null as RETURN_FLAG ,null as RETURNED_QUANTITY ,null as RETURNED_SALES ,null as CANCELLED_QUANTITY ,null as DAYS_IN_SHIPMENT ,null as ACQUSITION_DATE ,\"SKU\"::varchar as SKU_CODE ,\"Product Title/Description\"::varchar as PRODUCT_NAME_FINAL ,null as PRODUCT_CATEGORY ,null as PRODUCT_SUB_CATEGORY ,null as WAREHOUSE ,null as NEW_CUSTOMER_FLAG ,null as NEW_CUSTOMER_FLAG_MONTH ,null as ACQUISITION_PRODUCT ,null as ACQUISITION_CHANNEL ,null as ACQUISITION_MARKETPLACE from avorganics_db.MAPLEMONK.flipkart_sales;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from avorganics_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        