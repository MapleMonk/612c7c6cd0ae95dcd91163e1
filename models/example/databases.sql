{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE maplemonk.maplemonk.FACT_ITEMS_FEMORA AS SELECT \'Amazon_IN\' AS SHOP_NAME ,\"amazon-order-id\" AS ORDER_ID ,NULL AS ORDER_NAME ,NULL AS CUSTOMER_ID ,NULL AS LINE_ITEM_ID ,SKU ,ASIN AS MARKETPLACE_SKU_ID ,sku.sku_id AS PRODUCT_ID ,sku.sku_name AS PRODUCT_NAME ,sku.category as PRODUCT_CATEGORY ,sku.\"Sub Category\" as PRODUCT_SUB_CATEGORY ,sku.mrp as PRODUCT_MRP ,CURRENCY ,(case when \"order-status\" in (\'Return\',\'Shipped - Returned to Seller\',\'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\') then 1 else 0 end) AS IS_RETURN ,\"ship-city\" AS CITY ,\"ship-state\" AS STATE ,NULL AS CATEGORY ,\"order-status\" AS ORDER_STATUS ,\"purchase-date\":: DATETIME AS ORDER_TIMESTAMP ,TRY_CAST(\"item-price\" AS FLOAT) AS LINE_ITEM_SALES ,TRY_CAST(\"shipping-price\" AS FLOAT) AS SHIPPING_PRICE ,TRY_CAST(QUANTITY AS FLOAT) AS QUANTITY ,TRY_CAST(\"item-tax\" AS FLOAT) AS TAX ,TRY_CAST(\"item-promotion-discount\" AS FLOAT) AS DISCOUNT ,TRY_CAST(\"item-price\" AS FLOAT) AS NET_SALES ,\'Amazon\' AS SOURCE ,NULL AS LANDING_UTM_MEDIUM ,NULL AS LANDING_UTM_SOURCE ,NULL AS LANDING_UTM_CAMPAIGN ,NULL AS REFERRING_UTM_MEDIUM ,NULL AS REFERRING_UTM_SOURCE ,NULL AS LANDING_UTM_CHANNEL ,NULL AS REFERRING_UTM_CHANNEL ,NULL AS FINAL_UTM_CHANNEL ,NULL AS CUSTOMER_FLAG ,NULL AS NEW_CUSTOMER_FLAG ,NULL AS ACQUISITION_CHANNEL ,NULL AS ACQUISITION_PRODUCT ,TRY_CAST(\"shipping-tax\" AS FLOAT) AS SHIPPING_TAX ,TRY_CAST(\"ship-promotion-discount\" AS FLOAT) AS SHIP_PROMOTION_DISCOUNT ,TRY_CAST(\"gift-wrap-price\" AS FLOAT) AS GIFT_WRAP_PRICE ,TRY_CAST(\"gift-wrap-tax\" AS FLOAT) AS GIFT_WRAP_TAX ,asp._airbyte_emitted_at FROM maplemonk.maplemonk.ASP_IN_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL asp left join maplemonk.maplemonk.femora_sku_master sku on asp.asin = sku.amazon_asin and asp.sku=sku.sku_id WHERE \'order-status\' NOT IN(\'Cancelled\') AND \"item-price\" NOT IN(\'\',\'0.0\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MAPLEMONK.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        