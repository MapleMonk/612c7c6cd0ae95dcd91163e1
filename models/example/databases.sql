{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table slurp_db.maplemonk.sales_consolidated_slurp as select NULL as customer_id ,SHOP_NAME ,CARRIER_ID ,COURIER ,EMAIL ,CONTACT_NUM ,MARKETPLACE ,MARKETPLACE_ID ,ORDER_ID ,INVOICE_ID ,REFERENCE_CODE ,MANIFEST_DATE ,SHIPPING_LAST_UPDATE_DATE ,SHIPPING_STATUS ,SKU ,SKU_TYPE ,PRODUCT_ID ,PRODUCTNAME ,CURRENCY ,IS_REFUND ,CITY ,STATE ,PIN_CODE ,ORDER_STATUS ,ORDER_DATE ,SHIPPING_PRICE ,NUMBER_OF_PRODUCTS_IN_COMBO ,SUBORDER_QUANTITY ,SHIPPED_QUANTITY ,RETURNED_QUANTITY ,CANCELLED_QUANTITY ,RETURN_SALES ,CANCEL_SALES ,TAX ,SUBORDER_MRP ,MRP_SALES as MRP ,CATEGORY ,DISCOUNT ,SELLING_PRICE ,NEW_CUSTOMER_FLAG ,WAREHOUSE_NAME ,DAYS_IN_SHIPMENT ,PAYMENT_MODE ,IMPORT_DATE ,LAST_UPDATE_DATE ,AWB_NUMBER , MARKETPLACE as SOURCE, \'Synced\' as Easy_Ecom_Sync_Flag from slurp_db.maplemonk.easy_ecom_consolidated_slurp;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLURP_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        