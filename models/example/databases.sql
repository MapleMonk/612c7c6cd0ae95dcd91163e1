{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.TAG_ID_ANALYTICS AS with data1 as ( select TO_DATE(nullif(\"PO DATE\",\'\'),\'DD/MM/YYYY\') as PO_DATE, \"MFG DATE\", TO_DATE(nullif(\"PAYMENT DATE\",\'\'),\'DD/MM/YYYY\') as PAYMENT_DATE, nullif(\"APPROVAL DATE\",\'\') as APPROVAL_DATE, nullif(\"DISPATCH DATE\",\'\') as DISPATCH_DATE, nullif(\"LAST UPDATED DATE\",\'\') as LAST_UPDATED_DATE, nullif(\"ORDER CREATION DATE\",\'\') as ORDER_CREATION_DATE, nullif(\"EXPECTED DELIVERY DATE\",\'\') as EXPECTED_DELIVERY_DATE, nullif(\"PRINTING COMPLETED DATE\",\'\') as PRINTING_COMPLETED_DATE, \"MANUFACTURED MARKETED BY\", \"CUSTOMER PURCHASE ORDER\", \"PO EXTRA PERCENTAGE\", \"PRODUCT DESCRIPTION\", \"SHIPPED STATUS\", \"PAYMENT STATUS\", \"COURIER PARTNER\", \"AWB/LR NUMBER\", \"PO INITIAL QTY\"::INTEGER AS PO_INITIAL_QTY, \"NET QTY\" as NET_QTY, \"PO TOT QTY\"::INTEGER as PO_TOTAL_QTY, \"SKU NUMBER\", \"BRANCH NAME\", \"GROUP NAME6\", \"VENDOR CODE\", \"VENDOR NAME\", \"EAN NUMBER\", \"LOT NUMBER\", product, packname, \"TXN CODE\", \"COLOR NO\", sku, rate, size, color, chest, waist, inseem, sleeve, slogan, \"FC CODE\", \"EXCHANGE RATE\", \"CURRENCY NAME\" \"DELIVERY ADDRESS\", \"PRODUCT TAG TYPE\", \"BRANCH SHORT NAME\", \"PO UNIT MRP PRICE\" from snitch_db.maplemonk.s3_TagID ) select case when approval_date is null then \'APPROVAL_PENDING\' when dispatch_date is null then \'DISPATCH_PENDING\' else \'DISPATCHED_AWAITING_STATUS\' end as FINAL_STATUS , * from data1",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            