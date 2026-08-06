{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE redtape_db.maplemonk.returns_prime_refunds AS WITH gateway_shopify AS ( SELECT sku, f.value::STRING AS gateway FROM redtape_db.MAPLEMONK.redtape_db_SHOPIFY_FACT_ITEMS, LATERAL FLATTEN(input => gateway) f ), return_prime AS ( SELECT DISTINCT rp.order_name, rp.returned_sku AS article, rp.beneficiary_account_number, rp.item_price AS rp_amount, rp.name, rp.ifsc_code, rp.pincode, rp.city, rp.contact, rp.email, rp.awb, rp.shipping_courier AS logistics_partner, rp.return_facility_name AS warehouse FROM redtape_db.maplemonk.redtape_RETURNSPRIME_CONSOLIDATED rp LEFT JOIN gateway_shopify sp_gateway ON TRIM(UPPER(rp.returned_sku)) = TRIM(UPPER(sp_gateway.sku)) WHERE LOWER(rp.return_request_type) = \'return\' AND LOWER(rp.refund_Status) = \'pending\' AND LOWER(sp_gateway.gateway) IN (\'cash on delivery (cod)\',\'returnprime\') ), easyecom AS ( SELECT DISTINCT order_date::DATE AS order_date, reference_code, sku, selling_price AS ee_selling_price, payment_mode AS ee_payment_mode, FROM redtape_db.Maplemonk.redtape_db_EasyEcom_FACT_ITEMS ), clean AS ( SELECT ee.order_date, rp.order_name, rp.article, rp.beneficiary_account_number, rp.rp_amount, ee.ee_selling_price, ee.ee_selling_price - rp.rp_amount AS diff_amt, rp.rp_amount AS without_additional_charges, rp.name, rp.ifsc_code, rp.pincode, rp.city, rp.contact, rp.email, ee.ee_payment_mode, rp.awb, rp.logistics_partner, rp.warehouse FROM return_prime rp LEFT JOIN easyecom ee ON REPLACE(rp.order_name,\'#\',\'\') = ee.reference_code AND LOWER(rp.article) = LOWER(ee.sku) WHERE LOWER(CONCAT(REPLACE(rp.order_name,\'#\',\'\'), article)) NOT IN (SELECT LOWER(CONCAT(\"Order ID\", article)) FROM redtape_db.maplemonk.gs_neft_refund_kesar_) ) SELECT * FROM clean WHERE ee_payment_mode != \'PREPAID\'",
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
            