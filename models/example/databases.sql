{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table avorganics_db.maplemonk.swiggy_fact_items as select null as CUSTOMER_ID_FINAL ,null as ACQUISITION_DATE ,null as FIRST_COMPLETE_ORDER_DATE ,null as MAPLE_MONK_ID_PHONE ,null as CUSTOMER_ID ,\'Swiggy\' as SHOP_NAME ,\'Swiggy\' as MARKETPLACE ,\'Swiggy\' as CHANNEL ,\'Swiggy\' as SOURCE ,item_code::varchar as ORDER_ID ,item_code::varchar as REFERENCE_CODE ,null as PHONE ,null as NAME ,null as EMAIL ,null as SHIPPING_LAST_UPDATE_DATE ,item_code::varchar as SKU ,item_code::varchar as PRODUCT_ID ,product_name as PRODUCT_NAME ,null as CURRENCY ,city as CITY ,null as STATE ,null as ORDER_STATUS ,case when dt = \'\' then null else to_date(dt,\'dd-mm-yyyy\') end as ORDER_DATE ,total_quantity::float as QUANTITY ,null as GROSS_SALES_BEFORE_TAX ,null as DISCOUNT ,null as TAX ,null as SHIPPING_PRICE ,store_price*total_quantity::float as SELLING_PRICE ,null as OMS_ORDER_STATUS ,null as SHIPPING_STATUS ,null as FINAL_SHIPPING_STATUS ,null as SALEORDERITEMCODE ,null as SALES_ORDER_ITEM_ID ,null as AWB ,null as PAYMENT_GATEWAY ,null as PAYMENT_MODE ,null as COURIER ,null as DISPATCH_DATE ,case when dt = \'\' then null else to_date(dt,\'dd-mm-yyyy\') end DELIVERED_DATE ,null as DELIVERED_STATUS ,null as RETURN_FLAG ,null as RETURNED_QUANTITY ,null as RETURNED_SALES ,null as CANCELLED_QUANTITY ,null as DAYS_IN_SHIPMENT ,null as ACQUSITION_DATE ,item_code::varchar as SKU_CODE ,product_name as PRODUCT_NAME_FINAL ,L1_category as PRODUCT_CATEGORY ,L2_category as PRODUCT_SUB_CATEGORY ,null as WAREHOUSE ,null as NEW_CUSTOMER_FLAG ,null as NEW_CUSTOMER_FLAG_MONTH ,null as ACQUISITION_PRODUCT ,null as ACQUISITION_CHANNEL ,null as ACQUISITION_MARKETPLACE from avorganics_db.MAPLEMONK.s3_swiggy ;",
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
                        