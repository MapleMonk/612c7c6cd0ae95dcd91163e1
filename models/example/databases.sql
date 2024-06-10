{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table HOX_DB.MAPLEMONK.HOX_DB_Shopify_next_order as select distinct o.* from ( select distinct o.*, lead(order_Date) over(partition by phone_number order by order_Date, REFERENCE_CODE) as next_order_Date, lead(order_status) over(partition by phone_number order by order_Date, REFERENCE_CODE) as next_order_status, lead(REFERENCE_CODE) over(partition by phone_number order by order_Date, REFERENCE_CODE) as next_order_id from ( select distinct PHONE as phone_number, NAME as CUSTOMER_NAME, REFERENCE_CODE, date(ORDER_DATE) as order_Date, FINAL_SHIPPING_STATUS as order_status from HOX_DB.MAPLEMONK.HOX_DB_SALES_CONSOLIDATED where lower(MARKETPLACE) like \'%shopify%\' and (date(ORDER_DATE) between (current_date - Interval \'5 day\') and current_date) order by 1, 4, 3 )as o order by 1, 4 , 3 )as o where next_order_id is not null",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HOX_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        