{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table Anveshan_DB.maplemonk.sales_consolidated_Anveshan as select CUSTOMER_ID ,SHOP_NAME ,FINAL_UTM_CHANNEL AS SOURCE ,ORDER_ID ,PHONE ,NAME ,EMAIL ,NULL AS SHIPPING_LAST_UPDATE_DATE ,SKU ,PRODUCT_ID ,PRODUCT_NAME ,CURRENCY ,CITY ,STATE AS State ,ORDER_STATUS ,ORDER_TIMESTAMP::date AS Order_Date ,SHIPPING_PRICE ,QUANTITY ,DISCOUNT_BEFORE_TAX AS DISCOUNT ,TAX ,TOTAL_SALES AS SELLING_PRICE ,NULL AS SHIPPINGPACKAGECODE ,NULL AS SHIPPINGPACKAGESTATUS ,LINE_ITEM_ID::varchar as SALEORDERITEMCODE ,LINE_ITEM_ID as SALES_ORDER_ITEM_ID ,NULL AS COURIER ,NULL AS SHIPPING_STATUS ,NULL AS DISPATCH_DATE ,NULL AS DELIVERED_STATUS ,IS_REFUND AS RETURN_FLAG ,case when is_refund = 1 then quantity::int end returned_quantity ,case when is_refund = 0 and lower(order_status) in (\'cancelled\') then quantity::int end cancelled_quantity ,NEW_CUSTOMER_FLAG ,ACQUISITION_PRODUCT ,NULL AS DAYS_IN_SHIPMENT ,NULL AS ACQUSITION_DATE ,SKU_CODE ,PRODUCT_NAME_FINAL ,PRODUCT_CATEGORY ,LANGUAGE_CODE ,LOCAL_VARIANT ,ISO_3166_2_CODE ,SUBDIVISION_NAME ,PARENT_SUBDIVISION ,ROMANIZATION_SYSTEM ,SUBDIVISION_CATEGORY ,_AIRBYTE_AB_ID ,_AIRBYTE_EMITTED_AT ,_AIRBYTE_NORMALIZED_AT ,_AIRBYTE_REGION_ISO_3166_CODES_HASHID from Anveshan_DB.maplemonk.FACT_ITEMS_Shopify_Anveshan b union all select * from Anveshan_DB.maplemonk.UNICOMMERCE_FACT_ITEMS_ANVESHAN_FINAL_CRED;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from ANVESHAN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        