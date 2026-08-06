{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE redtape_db.maplemonk.returns_prime_refunds AS WITH gateway_shopify AS ( SELECT sku, f.value::STRING AS gateway FROM redtape_db.MAPLEMONK.redtape_db_SHOPIFY_FACT_ITEMS, LATERAL FLATTEN(input => gateway) f ), return_prime_1 AS ( SELECT DISTINCT RIGHT(rp.order_name,LENGTH(rp.order_name)-1) AS order_name, rp.returned_sku AS article, rp.beneficiary_account_number::STRING AS beneficiary_account_number, rp.item_price AS rp_amount, rp.name, rp.ifsc_code, rp.pincode, rp.city, rp.contact, rp.email, rp.awb, rp.shipping_courier AS logistics_partner, rp.return_facility_name AS warehouse, TRIM(UPPER(LEFT(rp.ifsc_code, 4))) AS bank_code FROM redtape_db.maplemonk.redtape_RETURNSPRIME_CONSOLIDATED rp LEFT JOIN gateway_shopify sp_gateway ON TRIM(UPPER(rp.returned_sku)) = TRIM(UPPER(sp_gateway.sku)) WHERE LOWER(rp.return_request_type) = \'return\' AND LOWER(rp.refund_Status) = \'pending\' AND LOWER(sp_gateway.gateway) IN (\'cash on delivery (cod)\',\'returnprime\') ), return_prime_2 AS ( SELECT rp1.*, bkname.\"Bank Name\" FROM return_prime_1 rp1 LEFT JOIN neft_googlesheet_bank_name bkname ON rp1.bank_code = bkname.\"Bank Code\" ), easyecom AS ( SELECT DISTINCT order_date::DATE AS order_date, reference_code, sku, selling_price AS ee_selling_price, payment_mode AS ee_payment_mode, FROM redtape_db.Maplemonk.redtape_db_EasyEcom_FACT_ITEMS ), clean AS ( SELECT ee.order_date, \'IN-TRANSIT\' AS status, rp2.order_name AS order_id, rp2.article, rp2.beneficiary_account_number, rp2.rp_amount, ee.ee_selling_price AS ee_amount, ee.ee_selling_price - rp2.rp_amount AS diff_amt, rp2.rp_amount AS without_additional_charges, rp2.name, rp2.ifsc_code, rp2.\"Bank Name\", rp2.pincode, rp2.city, rp2.contact, rp2.email, ee.ee_payment_mode AS rp_mop, ee.ee_payment_mode AS ee_mop, rp2.awb, rp2.logistics_partner, rp2.warehouse, CONCAT_WS(\'-\',rp2.order_name,rp2.article) AS merge FROM return_prime_2 rp2 LEFT JOIN easyecom ee ON REPLACE(rp2.order_name,\'#\',\'\') = ee.reference_code AND LOWER(rp2.article) = LOWER(ee.sku) WHERE LOWER(CONCAT(REPLACE(rp2.order_name,\'#\',\'\'), article)) NOT IN (SELECT LOWER(CONCAT(\"Order ID\", rp2.article)) FROM redtape_db.maplemonk.gs_neft_refund_kesar_) ) SELECT * FROM clean WHERE rp_mop != \'PREPAID\' AND order_date >= DATE \'2026-05-01\' ORDER BY order_date",
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
            