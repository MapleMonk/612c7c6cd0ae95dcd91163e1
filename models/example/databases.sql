{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_refund_management as with discount_percent as ( select distinct discount_codes, case when lower(discount_codes) like any (\'%buy4%\',\'%buy 4%\',\'%shop 4%\',\'%shop4%\',\'%shop any 4%\',\'%buy any 4%\',\'%4 pants%\',\'%4pala%\',\'%4pant%\',\'%4cotton%\') then 0.25 when lower(discount_codes) like any (\'%buy3%\',\'%buy 3%\',\'%shop 3%\',\'%shop3%\',\'%shop any 3%\',\'%buy any 3%\',\'%3 pants%\',\'%3pala%\',\'%3pant%\',\'%3cotton%\') then 0.20 when lower(discount_codes) like any (\'%buy2%\',\'%buy 2%\',\'%shop 2%\',\'%shop2%\',\'%shop any 2%\',\'%buy any 2%\',\'%2 pants%\',\'%2pala%\',\'%2pant%\',\'%2cotton%\') then 0.15 else 0 end as discount_percent from maplemonk.saadaa_returns_consolidated ), retained_orders as ( select reference_code, case when lower(discount_codes) like any (\'%rahosaadaa%\',\'%raho saadaa%\',\'%pehnosaadaa%\',\'%pehno saadaa%\') then 0.15 else 0 end as fixed_discount_percent, sum(case when return_flag = 0 or (return_flag = 1 and return_request_type = \'exchange\') then total_sales end) as retained_products_value, sum(case when (return_flag = 1 and return_request_type = \'return\') then total_sales end) as returned_products_value from maplemonk.saadaa_returns_consolidated group by 1,2 ), returned_orders as ( select r.reference_code as order_name, date(final_return_request_date) return_request_date, awb as forward_awb, return_awb as reverse_awb, concat(r.reference_code,return_awb) as unique_key, phone as customer_phone, customer_name, quantity_count as order_quanity, return_quantity_count, fixed_discount_percent, total_order_value, r.discount_codes, r.total_refund_amount, rp.retained_products_value/(1-dp.discount_percent) as original_retained_product_price, case when dp.discount_percent is not null then ( case when dp.discount_percent = 0.25 and (quantity_count - return_quantity_count) = 3 and date(final_return_request_date) < \'2025-07-10\' then 0.20 when dp.discount_percent = 0.25 and (quantity_count - return_quantity_count) >= 2 then 0.15 when dp.discount_percent = 0.20 and (quantity_count - return_quantity_count) >= 2 then 0.15 else 0 end ) else 0 end as new_discount_percent, returned_products_value, nector_coins_spent from maplemonk.saadaa_returns_consolidated r left join discount_percent dp on dp.discount_codes = r.discount_codes left join retained_orders rp on rp.reference_code = r.reference_code where lower(return_request_type) = \'return\' and lower(marketplace) like \'%website%\' and return_flag = 1 ) select *, (ifnull(total_order_value,0) - (ifnull(original_retained_product_price,0) * (1- coalesce(nullif(new_discount_percent,0), ifnull(fixed_discount_percent,0))))) as refund_value from returned_orders;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            