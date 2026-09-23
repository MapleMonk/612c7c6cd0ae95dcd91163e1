{{ config(
            materialized='table',
                post_hook={
                    "sql": "create table if not exists maplemonk.medmongers_flipkart_inventory ( location varchar, data_fetch_date date, company_token varchar, product_id varchar, product_name varchar, repair number(38,0), damaged number(38,0), received number(38,0), QC_Failed number(38,0), QC_Passed number(38,0), QC_Pending number(38,0), Total_Lost number(38,0), discard_fraud number(38,0), Available_Quantity number(38,0), Undispatched_Unassigned_Quantity number(38,0), rw number(38,0), ); create table if not exists maplemonk.medmongers_flipkart_daily_FSN_performance ( date date, sku varchar, units number(38,0), sales float, ad_spend float, ad_sales float, cllicks number(38,0), conversions float, acos float, roas float );",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            