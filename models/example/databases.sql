{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table avorganics_db.maplemonk.blinkit_fact_items as select null as CUSTOMER_ID_FINAL ,null as ACQUISITION_DATE ,null as FIRST_COMPLETE_ORDER_DATE ,null as MAPLE_MONK_ID_PHONE ,null as CUSTOMER_ID ,\'Blinkit\' as SHOP_NAME ,\'Blinkit\' as MARKETPLACE ,\'Blinkit\' as CHANNEL ,\'Blinkit\' as SOURCE ,item_id::varchar as ORDER_ID ,item_id::varchar as REFERENCE_CODE ,null as PHONE ,null as NAME ,null as EMAIL ,null as SHIPPING_LAST_UPDATE_DATE ,item_id::varchar as SKU ,item_id::varchar as PRODUCT_ID ,item_name as PRODUCT_NAME ,null as CURRENCY ,City_name as CITY ,null as STATE ,null as ORDER_STATUS ,case when date = \'\' then null else to_date(date,\'dd-mm-yyyy\') end as ORDER_DATE ,qty_sold:float as QUANTITY ,null as GROSS_SALES_BEFORE_TAX ,null as DISCOUNT ,null as TAX ,null as SHIPPING_PRICE ,mrp::float as SELLING_PRICE ,null as OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null as FINAL_SHIPPING_STATUS ,null as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,null as AWB ,null as PAYMENT_GATEWAY ,null as PAYMENT_MODE ,null as COURIER ,null as DISPATCH_DATE ,case when date = \'\' then null else to_date(date,\'dd-mm-yyyy\') end DELIVERED_DATE ,null as DELIVERED_STATUS ,null as RETURN_FLAG ,null as RETURNED_QUANTITY ,null as RETURNED_SALES ,null as CANCELLED_QUANTITY ,null as DAYS_IN_SHIPMENT ,null as ACQUSITION_DATE ,item_id::varchar as SKU_CODE ,item_name as PRODUCT_NAME_FINAL ,category as PRODUCT_CATEGORY ,null as PRODUCT_SUB_CATEGORY ,null as WAREHOUSE ,null as NEW_CUSTOMER_FLAG ,null as NEW_CUSTOMER_FLAG_MONTH ,null as ACQUISITION_PRODUCT ,null as ACQUISITION_CHANNEL ,null as ACQUISITION_MARKETPLACE from avorganics_db.MAPLEMONK.s3_blinkit ;",
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
                        