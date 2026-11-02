{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE justherbs.gokwik_email_orders_FactItems AS SELECT \"Shipping Charge\" AS shipping_charge, \"Item Subtotal\" AS item_subtotal, \"RTO Risk\" AS rto_risk, tax, \"Utm Source\" AS utm_source, \"Cod Charges\" AS cod_charges, \"Partial Paid Amount\" AS partial_paid_amount, \"Total Qty Ordered\" AS total_qty_ordered, \"Customer Email\" AS customer_email, \"Automatic Coupon Code\" AS automatic_coupon_code, \"Landing Page\" AS landing_page, \"Billing State\" AS billing_state, \"Post Edit Order Shipping\" AS post_edit_order_shipping, \"Variant ID\" AS variant_id, \"Payment Status\" AS payment_status, \"Line Item Id\" AS line_item_id, \"Utm Content\" AS utm_content, \"Merchant Order ID\" AS merchant_order_id, \"Shipping address\" AS shipping_address, REPLACE(\"Shopify Order Name\", \'#\', \'\') AS shopify_order_name, \"Total Discount\" AS total_discount, \"Loyalty Credits Used\" AS loyalty_credits_used, \"Billing Pincode\" AS billing_pincode, \"Created By\" AS created_by, \"RTO Remark\" AS rto_remark, \"Payment At\" AS payment_at, \"Total Item Count\" AS total_item_count, \"Utm Medium\" AS utm_medium, \"Loyalty Coins Used\" AS loyalty_coins_used, \"Customer Phone\" AS customer_phone, \"Billing address\" AS billing_address, \"Line Item Statuses\" AS line_item_statuses, \"Merchant Order Status\" AS merchant_order_status, \"Billing City\" AS billing_city, \"Payment Method\" AS payment_method, \"Customer Name\" AS customer_name, \"Utm Term\" AS utm_term, TRY_TO_DATE(\"Created At\", \'DD/MM/YYYY HH12:MI AM\') AS created_date, TRY_TO_DATE(\"Updated At\", \'DD/MM/YYYY HH12:MI AM\') AS Updated_Date, \"Grand Total\" AS grand_total, \"Store Source\" AS store_source, \"Gift Card\" AS gift_card, \"Payment ID\" AS payment_id, \"Product Name\" AS product_name, \"RTO Score\" AS rto_score, \"Post Edit Order Discount\" AS post_edit_order_discount, \"Convenience Fee\" AS convenience_fee, \"Utm Campaign\" AS utm_campaign, \"Gift Card Amount\" AS gift_card_amount, \"Payment Discount\" AS payment_discount, \"Coupon Code Based Discount\" AS coupon_code_based_discount, \"AWB string\" AS awb_string, \"Order string\" AS order_string, FROM justherbs.gokwik_orders_jh_csv_from_mail;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DATALAKE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            