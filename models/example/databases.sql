{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table RPSG_DB.MAPLEMONK.CUSTOMRES_DATA as SELECT order_date::date AS date, REFERENCE_CODE, NEW_CUSTOMER_FLAG, CASE WHEN UPPER(NEW_CUSTOMER_FLAG) LIKE \'%YET%\' THEN \'NEW\' ELSE UPPER(NEW_CUSTOMER_FLAG) END AS CUSTOMER_FLAG, FROM rpsg_DB.maplemonk.SALES_CONSOLIDATED_DRV WHERE LOWER(marketplace) = \'shopify_drv\'",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            