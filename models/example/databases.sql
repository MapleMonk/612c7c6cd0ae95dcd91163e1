{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.discount_data as ( with discount_data as ( SELECT order_timestamp::DATE AS date, discount_code, pincode, discount, gross_sales, CASE WHEN LOWER(payment_method) = \'cod\' THEN \'cod\' WHEN LOWER(payment_method) = \'prepaid\' THEN \'prepaid\' ELSE \'exchange\' END AS method, sku_group, category, metro, new_customer_flag, CASE WHEN lower(webshopney) = \'appbrew\' THEN \'app\' ELSE \'web\' END AS type FROM snitch_db.maplemonk.fact_items_snitch ), row_num as ( select *, ROW_NUMBER() OVER (PARTITION BY pincode ORDER BY \"Office Name\" DESC) AS row_num from snitch_db.maplemonk.pincode_mapping ) select a.*, b.statename from discount_data a left join row_num b on a.pincode = b.pincode where b.row_num = 1 ); create or replace table snitch_db.maplemonk.discount_data_statewise as ( SELECT statename, SUM(discount) AS discount, SUM(gross_sales) AS gross_sales, (SUM(discount) / (SUM(gross_sales) + SUM(discount))) * 100 AS discount_percentage, (SUM(gross_sales) / (SELECT SUM(gross_sales) FROM snitch_db.maplemonk.discount_data WHERE date BETWEEN CURRENT_DATE - INTERVAL \'30 days\' AND CURRENT_DATE)) * 100 AS sales_percentage FROM snitch_db.maplemonk.discount_data WHERE date BETWEEN CURRENT_DATE - INTERVAL \'30 days\' AND CURRENT_DATE GROUP BY statename ORDER BY discount_percentage DESC ); create or replace table snitch_db.maplemonk.discount_data_categorywise as ( SELECT category, SUM(discount) AS discount, SUM(gross_sales) AS gross_sales, (SUM(discount) / (SUM(gross_sales) + SUM(discount))) * 100 AS discount_percentage, (SUM(gross_sales) / (SELECT SUM(gross_sales) FROM snitch_db.maplemonk.discount_data WHERE date BETWEEN CURRENT_DATE - INTERVAL \'30 days\' AND CURRENT_DATE)) * 100 AS sales_percentage FROM snitch_db.maplemonk.discount_data WHERE date BETWEEN CURRENT_DATE - INTERVAL \'30 days\' AND CURRENT_DATE GROUP BY category ORDER BY discount_percentage DESC );",
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
                        