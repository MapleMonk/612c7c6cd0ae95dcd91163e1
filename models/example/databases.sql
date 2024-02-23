{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table avorganics_db.maplemonk.flipkart_fact_items as select null as CUSTOMER_ID_FINAL ,null as ACQUISITION_DATE ,null as FIRST_COMPLETE_ORDER_DATE ,null as MAPLE_MONK_ID_PHONE ,null as CUSTOMER_ID ,\'Flipkart\' as SHOP_NAME ,\'Flipkart\' as MARKETPLACE ,\'Flipkart\' as CHANNEL ,\'Flipkart\' as SOURCE ,_airbyte_data:\"Order ID\"::varchar as ORDER_ID ,_airbyte_data:\"Order ID\"::varchar as REFERENCE_CODE ,null as PHONE ,null as NAME ,null as EMAIL ,null as SHIPPING_LAST_UPDATE_DATE ,b.sku as SKU ,replace(replace(_airbyte_data:\"FSN\",\'\"\',\'\'),\'\\'\',\'\') as PRODUCT_ID ,replace(replace(_airbyte_data:\"Product Title/Description\",\'\"\',\'\'),\'\\'\',\'\') as PRODUCT_NAME ,null as CURRENCY ,null as CITY ,replace(_airbyte_data:\"Customer\'s Delivery State\",\'\"\',\'\') as STATE ,replace(_airbyte_data:\"Event Type\",\'\"\',\'\') as ORDER_STATUS ,left(replace(_airbyte_data:\"Order Date\",\'\"\',\'\'),10)::date ORDER_DATE ,_airbyte_data:\"Item Quantity\"::float as QUANTITY ,null as GROSS_SALES_BEFORE_TAX ,replace(_airbyte_data:\"Total Discount\",\'-\',\'\')::float as DISCOUNT ,null as TAX ,_airbyte_data:\"Shipping Charges\"::float as SHIPPING_PRICE ,_airbyte_data:\"Buyer Invoice Amount \"::float as SELLING_PRICE ,null as OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null as FINAL_SHIPPING_STATUS ,null as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,null as AWB ,null as PAYMENT_GATEWAY ,null as PAYMENT_MODE ,null as COURIER ,null as DISPATCH_DATE ,left(replace(_airbyte_data:\"Order Date\",\'\"\',\'\'),10)::date DELIVERED_DATE ,null as DELIVERED_STATUS ,null as RETURN_FLAG ,null as RETURNED_QUANTITY ,null as RETURNED_SALES ,null as CANCELLED_QUANTITY ,null as DAYS_IN_SHIPMENT ,null as ACQUSITION_DATE ,replace(replace(replace(_airbyte_data:\"SKU\",\'\"\',\'\'),\'\\'\',\'\'),\'SKU:\',\'\') as SKU_CODE ,b.product_name as PRODUCT_NAME_FINAL ,b.category as PRODUCT_CATEGORY ,b.sub_category as PRODUCT_SUB_CATEGORY ,null as WAREHOUSE ,null as NEW_CUSTOMER_FLAG ,null as NEW_CUSTOMER_FLAG_MONTH ,null as ACQUISITION_PRODUCT ,null as ACQUISITION_CHANNEL ,null as ACQUISITION_MARKETPLACE from avorganics_db.maplemonk._airbyte_raw_s3_flipkart_raw a left join (select sku, product_code, product_name, category, sub_category from (select primarykey sku, \"FLIPKART FSN\" product_code, \"PRODUCT TITLE\" product_name, category, sub_category, row_number() over (partition by \"FLIPKART FSN\" order by 1) rw from avorganics_db.maplemonk.sku_master where \"FLIPKART FSN\" <>\'-\' )where rw = 1) b on replace(replace(_airbyte_data:\"FSN\",\'\"\',\'\'),\'\\'\',\'\') = b.product_code",
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
                        