{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table redtape_db.maplemonk.returns_prime_refunds as with returns_prime as ( select distinct order_name, returned_sku article, beneficiary_account_number, item_price RP_amount, name, ifsc_Code, pincode, city, contact, email, awb, shipping_courier logistics_partner, return_facility_name warehouse from redtape_db.maplemonk.redtape_RETURNSPRIME_CONSOLIDATED where lower(return_request_type) = \'return\' and lower(refund_Status) = \'pending\' and awb is not null ), easyecom as ( select distinct order_Date::date order_Date, reference_Code, sku, selling_price EE_selling_price, payment_mode EE_payment_mode, from redtape_db.Maplemonk.redtape_db_EasyEcom_FACT_ITEMS ) select r.*, order_date, EE_selling_price, EE_payment_mode from returns_prime r left join easyecom e on replace(r.ordeR_name, \'#\',\'\') = e.reference_Code and lower(r.article) = lower(e.sku) where lower(concat(replace(r.ordeR_name, \'#\',\'\'), article)) not in (select lower(concat(\"Order ID\", article)) from redtape_db.maplemonk.gs_neft_refund_kesar_) and EE_payment_mode = \'COD\' ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from REDTAPE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            