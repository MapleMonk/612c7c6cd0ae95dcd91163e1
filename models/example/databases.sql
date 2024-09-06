{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_amazon_UK_fact_items AS SELECT \'AMAZON_UK\' AS shop_name, \"amazon-order-id\" AS order_id, NULL AS order_name, NULL AS customer_id, NULL AS phone, NULL AS tags, NULL AS line_item_id, sku, ASIN AS product_id, currency, case when \"order-status\" in (\'Shipped - Returned to Seller\', \'Shipped - Returning to Seller\',\'Shipped - Rejected by Buyer\',\'Shipped - Damaged\') then 1 else 0 end AS is_refund, upper(\"ship-city\") AS city, upper(\"ship-state\") AS state, \"ship-postal-code\" AS pincode, \"product-name\" AS product_name, NULL AS category, \"order-status\" AS order_status, \"Purchase-datetime-PDT\" AS order_timestamp, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) AS line_item_sales, ifnull(TRY_CAST(\"shipping-price\" AS FLOAT),0) AS shipping_price, ifnull(TRY_CAST(QUANTITY AS FLOAT),0) AS quantity, ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0) AS tax, div0(ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0),ifnull(TRY_CAST(\"item-tax\" AS FLOAT),0)+ifnull(TRY_CAST(\"item-price\" AS FLOAT),0)) as tax_rate, ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS discount, div0(ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0),(1-tax_rate)) AS discount_before_tax, NULL AS gross_sales_after_tax, line_item_sales - discount_before_tax AS gross_sales_before_tax, NULL AS net_sales_before_tax, ifnull(TRY_CAST(\"item-price\" AS FLOAT),0) - ifnull(TRY_CAST(\"item-promotion-discount\" AS FLOAT),0) AS total_sales, \'Amazon\' AS source, NULL AS landing_utm_medium, NULL AS landing_utm_source, NULL AS landing_utm_campaign, NULL AS referring_utm_medium, NULL AS referring_utm_source, NULL AS landing_utm_channel, NULL AS referring_utm_channel, NULL AS final_utm_channel, NULL AS new_customer_flag, NULL AS acquisition_channel, NULL AS acquisition_product, ifnull(TRY_CAST(\"shipping-tax\" AS FLOAT),0) AS shipping_tax, ifnull(TRY_CAST(\"ship-promotion-discount\" AS FLOAT),0) AS ship_promotion_discount, ifnull(TRY_CAST(\"gift-wrap-price\" AS FLOAT),0) AS gift_wrap_price, ifnull(TRY_CAST(\"gift-wrap-tax\" AS FLOAT),0) AS gift_wrap_tax FROM (SELECT *, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\', \"purchase-date\":: DATETIME) as \"Purchase-datetime-PDT\" FROM ( select * FROM VAHDAM_DB.MAPLEMONK.ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL union all (select * FROM VAHDAM_DB.MAPLEMONK.ASP_UK_historical_data_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL where not(\"amazon-order-id\" in ( select distinct \"amazon-order-id\" FROM VAHDAM_DB.MAPLEMONK.ASP_UK_GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL ))) ) )X ; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_amazon_UK_fact_items_TEMP_Category as select fi.*, smg.\"COMMON SKU ID\" as Common_SKU, smg.\"Common SKU Description\" as PRODUCT_NAME_FINAL, coalesce(smg.CATEGORY,fi.category) as Product_CATEGORY, smg.\"SUB CATEGORY\" as \"Category_2-Type_of_Tea\", smg.\"LOOSE/TEA BAG/ POWDER\" as \"Category_3-Type_of_Product\", null as \"Category_4-Pack_type\", smg.\"Mother SKU\", smg.weight, smg.\"Amazon UK MSRP\", smg.BRAND from VAHDAM_DB.MAPLEMONK.VAHDAM_DB_amazon_UK_fact_items fi left join (select \"Amazon UK\" ,weight ,brand ,\"Mother SKU\" ,\"Common Name\" ,category ,\"SUB CATEGORY\" ,\"LOOSE/TEA BAG/ POWDER\" ,\"Common SKU Description\" ,\"COMMON SKU ID\" ,\"Amazon UK MSRP\" ,row_number() over (partition by \"Amazon UK\" order by \"Amazon UK\") as rw from vahdam_db.maplemonk.sku_mapping_raw_data) smg on fi.product_id = smg.\"Amazon UK\" ; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.VAHDAM_DB_amazon_UK_fact_items AS SELECT * FROM VAHDAM_DB.MAPLEMONK.VAHDAM_DB_amazon_UK_fact_items_TEMP_Category;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from VAHDAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            