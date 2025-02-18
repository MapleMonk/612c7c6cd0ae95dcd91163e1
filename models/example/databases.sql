{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create OR REPLACE table rpsg_db.maplemonk.Consultations_Age_Gender as SELECT DATE_TRUNC(\'DAY\', s.\"ORDER_DATE\") AS ORDER_DATE, s.\"SHOP_NAME\" AS SHOP_NAME, s.\"REFERENCE_CODE\" AS REFERENCE_CODE, s.\"ORDER_ID\" AS ORDER_ID, s.\"SKU\" AS SKU, s.\"PRODUCT_NAME_MAPPED\" AS PRODUCT_NAME_MAPPED, s.\"SELLING_PRICE\" AS SELLING_PRICE, s.\"MRP_SALES\" AS MRP_SALES, s.\"INVOICE_DATE\" AS INVOICE_DATE, s.\"AWB\" AS AWB, s.\"CUSTOMER_NAME\" AS CUSTOMER_NAME, s.\"PHONE\" AS PHONE, s.\"CUSTOMER_ID_FINAL\" AS CUSTOMER_ID_FINAL, s.\"EMAIL\" AS EMAIL, s.\"ORDER_STATUS\" AS ORDER_STATUS, s.\"MAPPED_STATE\" AS MAPPED_STATE, s.\"MAPPED_CITY\" AS MAPPED_CITY, s.\"CITY\" AS CITY, s.\"FINAL_STATUS\" AS FINAL_STATUS, s.\"STATE\" AS STATE, s.\"APPOINTMENT_ID\" AS APPOINTMENT_ID, b.\"YEAR_OF_BIRTH\" AS YearOfBirth, b.GENDER, EXTRACT(YEAR FROM CURRENT_DATE) - b.\"YEAR_OF_BIRTH\" AS Age FROM rpsg_db.maplemonk.sales_consolidated_three60 s JOIN rpsg_db.maplemonk.pg_three60you_customer b ON s.\"PHONE\" = b.\"PHONE\" WHERE lower(s.SHOP_NAME) = \'consultations\' AND LOWER(s.\"DATA_SOURCE\") = \'three60\'",
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
            