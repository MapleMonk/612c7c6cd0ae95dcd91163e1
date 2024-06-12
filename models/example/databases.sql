{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.affiliate_cod as ( with affiliate as ( select distinct order_name,sales,discount,partner_af from snitch_db.maplemonk.affiliate_snitch ), row_num as ( select *, ROW_NUMBER() OVER (PARTITION BY pincode ORDER BY \"Office Name\" DESC) AS row_num from snitch_db.maplemonk.pincode_mapping ), sales_data as ( select distinct a.order_name,a.order_timestamp::date as order_date,a.payment_channel,a.pincode,b.statename from snitch_db.maplemonk.fact_items_snitch a left join row_num b on a.pincode = b.pincode where b.row_num = 1 ) select a.partner_af,a.sales,a.discount,b.* from affiliate a inner join sales_data b on a.order_name = b.order_name );",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        