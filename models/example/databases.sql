{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE hox_db.maplemonk.AMAZON_DAILY_WEEKELY_MONTHLY_METRICS AS SELECT ASIN, DATE, PRODUCT_NAME, CASE WHEN SPEND=\'Not Applicable\' THEN NULL ELSE SPEND END AS FINAL_SPEND, CASE WHEN UNITS=\'Not Applicable\' THEN NULL ELSE UNITS END AS FINAL_UNITS, CASE WHEN REVENUE=\'Not Applicable\' THEN NULL ELSE REVENUE END AS FINAL_REVENUE, DATE_TRUNC(\'WEEK\',TO_DATE(DATE,\'yyyy/mm/dd\')) as week_date, DATE_TRUNC(\'MONTH\',TO_DATE(DATE,\'yyyy/mm/dd\')) as month_date FROM pradhuman_requirements_jan_units;",
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
            