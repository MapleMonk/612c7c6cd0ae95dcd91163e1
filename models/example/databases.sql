{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table datalake_db.kaya.dwh_order_distribution as select customer_id, order_timestamp::date order_Date, ordeR_name, sum(total_sales) sales, case when sum(total_sales) >= 0 and sum(total_sales) < 100 then \'01 00-100\' when sum(total_sales) >= 100 and sum(total_sales) < 200 then \'02 100-200\' when sum(total_sales) >= 200 and sum(total_sales) < 300 then \'03 200-300\' when sum(total_sales) >= 300 and sum(total_sales) < 400 then \'04 300-400\' when sum(total_sales) >= 400 and sum(total_sales) < 500 then \'05 400-500\' when sum(total_sales) >= 500 and sum(total_sales) < 600 then \'06 500-600\' when sum(total_sales) >= 600 and sum(total_sales) < 700 then \'07 600-700\' when sum(total_sales) >= 700 and sum(total_sales) < 800 then \'08 700-800\' when sum(total_sales) >= 800 and sum(total_sales) < 900 then \'09 800-900\' when sum(total_sales) >= 900 and sum(total_sales) < 1000 then \'10 900-1000\' when sum(total_sales) >= 1000 and sum(total_sales) < 1100 then \'11 1000-1100\' when sum(total_sales) >= 1100 and sum(total_sales) < 1200 then \'12 1100-1200\' when sum(total_sales) >= 1200 and sum(total_sales) < 1300 then \'13 1200-1300\' when sum(total_sales) >= 1300 and sum(total_sales) < 1400 then \'14 1300-1400\' when sum(total_sales) >= 1400 and sum(total_sales) < 1500 then \'15 1400-1500\' when sum(total_sales) >= 1500 and sum(total_sales) < 1600 then \'16 1500-1600\' when sum(total_sales) >= 1600 and sum(total_sales) < 1700 then \'17 1600-1700\' when sum(total_sales) >= 1700 and sum(total_sales) < 1800 then \'18 1700-1800\' when sum(total_sales) >= 1800 and sum(total_sales) < 1900 then \'19 1800-1900\' when sum(total_sales) >= 1900 and sum(total_sales) < 2000 then \'20 1900-2000\' when sum(total_sales) >= 2000 and sum(total_sales) < 2100 then \'21 2000-2100\' when sum(total_sales) >= 2100 and sum(total_sales) < 2200 then \'22 2100-2200\' when sum(total_sales) >= 2200 and sum(total_sales) < 2300 then \'23 2200-2300\' when sum(total_sales) >= 2300 and sum(total_sales) < 2400 then \'24 2300-2400\' when sum(total_sales) >= 2400 and sum(total_sales) < 2500 then \'25 2400-2500\' when sum(total_sales) >= 2500 then \'26 >2500\' end as sales_bucket from datalake_db.kaya.dwh_SHOPIFY_FACT_ITEMS group by 1, 2, 3;",
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
            