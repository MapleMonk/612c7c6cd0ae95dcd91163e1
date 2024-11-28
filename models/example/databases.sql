{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create OR REPLACE table rpsg_db.maplemonk.RTO_Assisted1 as SELECT REFERENCE_CODE AS \"ORDER_ID\", customer_phone, shop_name, product_name_mapped, category, TO_DATE(return_date) AS \"Return_Date\", company_name, return_reason, total_return_amount, name, email, mapped_city, mapped_state FROM RPSG_DB.MAPLEMONK.fact_items_easyecom_returns_detailed_three60 WHERE UPPER(SHOP_NAME) = \'CONSULTATIONS\';",
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
            