{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE ORPAT_DB.MAPLEMONK.ORPAT_WEBSITE_FACT_ITEMS AS WITH ORDER_UTM_PARAMETERS AS (select * from (select ID ,UPPER(MEDIUM) MEDIUM ,UPPER(SOURCE) SOURCE ,UPPER(CONTENT) CONTENT ,UPPER(CAMPAIGN) CAMPAIGN ,ORDER_ID ,try_to_timestamp(created_at) CREATED_AT ,row_number() over (partition by ORDER_ID order by try_to_timestamp(created_at) desc) rw from orpat_db.maplemonk.orpat_mysql_order_source ) where rw = 1 ), CUSTOMERS AS ( select * from ( select CUSTOMER_ID ,USER_ID ,EMAIL ,USERNAME ,UPPER(LAST_NAME) LAST_NAME ,UPPER(FIRST_NAME) FIRST_NAME ,POSTCODE ,UPPER(CITY) CITY ,UPPER(STATE) STATE ,UPPER(COUNTRY) COUNTRY ,TRY_TO_TIMESTAMP(DATE_REGISTERED) DATE_REGISTERED ,TRY_TO_TIMESTAMP(DATE_LAST_ACTIVE) DATE_LAST_ACTIVE , row_number() over (partition by CUSTOMER_ID order by TRY_TO_TIMESTAMP(DATE_LAST_ACTIVE) desc) rw from orpat_db.maplemonk.orpat_mysql_xrwvndsq_wc_customer_lookup ) where rw = 1 ), ORDER_DETAILS AS ( SELECT * FROM (Select ORDER_ID ,PARENT_ID ,CUSTOMER_ID ,RETURNING_CUSTOMER ,UPPER(STATUS) ORDER_STATUS ,TRY_TO_TIMESTAMP(DATE_CREATED) DATE_CREATED ,TRY_TO_TIMESTAMP(DATE_CREATED_GMT) DATE_CREATED_GMT ,TRY_TO_TIMESTAMP(DATE_COMPLETED) DATE_COMPLETED ,TRY_TO_TIMESTAMP(DATE_PAID) PAYMENT_DATE ,NET_TOTAL ,TAX_TOTAL ,SHIPPING_TOTAL ,TOTAL_SALES ,NUM_ITEMS_SOLD ,row_number() over (partition by ORDER_ID order by TRY_TO_TIMESTAMP(DATE_CREATED) desc) rw from orpat_db.maplemonk.orpat_mysql_xrwvndsq_wc_order_stats ) where rw = 1 ), PRODUCT_SKU as ( SELECT * FROM( SELECT PRODUCT_ID ,SKU ,row_number() over (partition by product_id order by 1) rw FROM orpat_db.maplemonk.ORPAT_MYSQL_XRWVNDSQ_WC_PRODUCT_META_LOOKUP ) where rw = 1 ) Select OL.ORDER_ID ,OL.ORDER_ITEM_ID ,replace(OD.ORDER_STATUS,\'WC-\',\'\') ORDER_STATUS ,OL.CUSTOMER_ID ,C.EMAIL ,CONCAT(C.FIRST_NAME, C.LAST_NAME) NAME ,C.POSTCODE PINCODE ,UPPER(C.CITY) CITY ,UPPER(C.STATE) STATE ,UPPER(C.COUNTRY) COUNTRY ,try_to_timestamp(OL.DATE_CREATED) DATE_CREATED ,OD.PAYMENT_DATE ,OD.DATE_COMPLETED ,UTM.MEDIUM ,UTM.SOURCE ,UTM.CONTENT ,UTM.CAMPAIGN ,OL.PRODUCT_ID ,P.SKU ,OL.VARIATION_ID ,OL.PRODUCT_QTY QUANTITY ,OL.COUPON_AMOUNT ,OL.PRODUCT_GROSS_REVENUE ,OL.TAX_AMOUNT ,OL.PRODUCT_NET_REVENUE ,OL.SHIPPING_AMOUNT ,OL.SHIPPING_TAX_AMOUNT from orpat_db.maplemonk.orpat_mysql_xrwvndsq_wc_order_product_lookup ol left join CUSTOMERS C on OL.CUSTOMER_ID = C.CUSTOMER_ID left join PRODUCT_SKU P on OL.PRODUCT_ID = P.PRODUCT_ID left join ORDER_DETAILS OD on OL.ORDER_ID = OD.ORDER_ID left join ORDER_UTM_PARAMETERS UTM on OL.ORDER_ID = UTM.ORDER_ID ;",
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
            