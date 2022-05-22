{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table XYXX_DB.maplemonk.sales_consolidated_XYXX as select CUSTOMER_ID ,SHOP_NAME ,FINAL_UTM_CHANNEL AS SOURCE ,ORDER_ID ,PHONE ,NAME ,EMAIL ,NULL AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE ,ORDER_STATUS ,ORDER_TIMESTAMP::date AS Order_Date ,SHIPPING_PRICE ,QUANTITY ,DISCOUNT ,TAX ,NET_SALES ,NULL AS SHIPPINGPACKAGECODE ,NULL AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::Varchar ,LINE_ITEM_ID ,NULL AS COURIER ,NULL AS SHIPPING_STATUS ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,NULL AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY from XYXX_db.maplemonk.FACT_ITEMS_XYXX b union all select * from XYXX_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_XYXX_FINAL where lower(marketplace) not like (\'%amazon%\') and lower(marketplace) not like (\'%shopify%\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from XYXX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        