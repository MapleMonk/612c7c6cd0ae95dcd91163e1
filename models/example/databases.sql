{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ORPAT_DB.MAPLEMONK.ORPAT_SNAPDEAL_FACT_ITEMS AS select \"SUPC\" as SUPC ,\"AWB NO\" as AWB_NO ,upper(\"SKU CODE\") as SKU_CODE ,upper(\"BUYER NAME\") as BUYER_NAME ,\"ORDER CODE\" as ORDER_CODE ,coalesce(try_to_date(\"ORDER DATE\", \'DD MONTH YYYY\'), try_to_date(\"ORDER DATE\", \'DD MON YYYY\'), try_to_date(\"ORDER DATE\")) as ORDER_DATE ,\"PAYMENT MODE\" as PAYMENT_MODE ,upper(\"PRODUCT NAME\") as PRODUCT_NAME ,\"TRACKING URL\" as TRACKING_URL ,try_cast(\"SELLING PRICE\" as float) as SELLING_PRICE ,upper(\"SHIPPING CITY\") as SHIPPING_CITY ,\"SUBORDER CODE\" as SUBORDER_CODE ,upper(\"CUSTOMER STATE\") as CUSTOMER_STATE ,\"REFERENCE CODE\" as REFERENCE_CODE ,\"COURIER PARTNER\" as COURIER_PARTNER ,\"CUSTOMER PINCODE\" as CUSTOMER_PINCODE ,\"DELIVERY ADDRESS\" as DELIVERY_ADDRESS ,upper(\"PRODUCT CATEGORY\") as PRODUCT_CATEGORY ,\"MANIFEST/HOS/STN CODE\" as MANIFEST_HOS_STN_CODE ,_AB_SOURCE_FILE_URL ,_AB_SOURCE_FILE_LAST_MODIFIED from orpat_db.maplemonk.orpat_s3_snapdeal;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ORPAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            