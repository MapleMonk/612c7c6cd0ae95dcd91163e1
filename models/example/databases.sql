{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table xyxx_db.maplemonk.payment_details_summary as with snapmint as (select c.order_name, b.ordeR_Date, a.payment_id, a.fee, a.tax, (a.fee + a.tax) total_fee, null as discount from (select \"Order ID\" payment_id, \"Total subvention\" fee, \"GST @18%\" tax from xyxx_db.maplemonk.snapmint_payments where to_date(\"Merchant Settlement Date\",\'dd-mm-yyyy\') <= \'2023-08-31\' union all select distinct \"Order ID\" payment_id, \"Total subvention\" fee, \"GST @18%\" tax from xyxx_db.maplemonk.snapmint_payments_sep_2023 ) a left join ( select * from xyxx_db.maplemonk.shopify_all_payment_details where lower(payment_gateway) like \'%snap%\' )c on a.payment_id = c.payment_id left join (select distinct ordeR_name, order_timestamp::date order_date from xyxx_db.maplemonk.fact_items_xyxx) b on c.order_name = b.ordeR_name where b.ordeR_name is not null ) , razorpay as ( select a.order_name, b.order_date, a.payment_id, transaction_fee fee, transaction_tax tax, transaction_fee + transaction_tax total_fee, null as discount from xyxx_db.maplemonk.shopify_all_payment_details a left join (select distinct ordeR_name, order_timestamp::date order_date from xyxx_db.maplemonk.fact_items_xyxx) b on a.order_name = b.ordeR_name where lower(payment_gateway) like \'%razor%\' and b.ordeR_name is not null and a.created_date::date between \'2023-01-01\' and \'2024-07-31\' ) , CRED as ( select ordeR_name, ordeR_date, payment_id, sum(fee) as fee, sum(tax) as tax, sum(total_fee) as total_fee, sum(discount) discount from ( select c.order_name, b.ordeR_date, a.payment_id, a.fee, a.tax, a.total_fee, discount from ( select distinct MERCHANT_ORDER_ID payment_id, offer_amount discount,(cred_revenue_share) fee, tax_on_revenue_share tax, (cred_revenue_share + tax_on_revenue_share) total_fee from xyxx_db.maplemonk.cred_payments where left(settlement_time,10)::date <= \'2023-08-31\' union all select distinct MERCHANT_ORDER_ID payment_id, offer_amount discount, (cred_revenue_share) fee, tax_on_revenue_share tax, (cred_revenue_share + tax_on_revenue_share) total_fee from xyxx_db.maplemonk.cred_payments_sep_2023 ) a left join ( select * from xyxx_db.maplemonk.shopify_all_payment_details where lower(payment_gateway) like \'%cred%\' )c on a.payment_id = c.payment_id left join (select distinct ordeR_name, order_timestamp::date order_date from xyxx_db.maplemonk.fact_items_xyxx) b on c.order_name = b.ordeR_name where b.ordeR_name is not null ) group by 1,2,3 ) select \'Razorpay\' as payment_partner, * from razorpay union all select \'Snapmint\' as payment_partner, * from snapmint union all select \'CRED\' as payment_partner, * from cred ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from XYXX_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            