{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.demographics_data_sales as WITH pincode_mapping AS ( SELECT UPPER(state) AS state, district, pincode, \"ISO CODE\" as iso_code, zone as zone FROM snitch_db.maplemonk.gs_pincode_mapping_lat_long_tier QUALIFY ROW_NUMBER() OVER (PARTITION BY pincode ORDER BY state DESC) = 1 ), sales_data AS ( SELECT order_timestamp::date as order_date, a.pincode, b.state, b.district, b.iso_code, b.zone, sum(gross_sales) as gross_sales FROM snitch_db.maplemonk.fact_items_snitch a left join pincode_mapping b on a.pincode::varchar = b.pincode::varchar WHERE LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') and lower(tags) not like \'%creator%\' group by 1,2,3,4,5,6 ), weekly_agg AS ( SELECT DATE_TRUNC(\'week\', order_date) AS week_start_date, pincode, state, district, iso_code, zone, SUM(gross_sales) AS weekly_gross_sales FROM sales_data GROUP BY 1,2,3,4,5,6 ), monthly_agg AS ( SELECT DATE_TRUNC(\'month\', order_date) AS month_start_date, pincode, state, district, iso_code, zone, SUM(gross_sales) AS monthly_gross_sales FROM sales_data GROUP BY 1,2,3,4,5,6 ) select sd.order_date, sd.pincode, sd.state, sd.district, sd.iso_code, sd.zone, sd.gross_sales, DENSE_RANK() OVER (ORDER BY DATE_TRUNC(\'week\', sd.order_date)) AS week_rank, DENSE_RANK() OVER (ORDER BY DATE_TRUNC(\'month\', sd.order_date)) AS month_rank, AVG(sd.gross_sales) OVER (ORDER BY sd.order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7_day_avg, AVG(sd.gross_sales) OVER (ORDER BY sd.order_date ROWS BETWEEN 14 PRECEDING AND 7 PRECEDING) AS rolling_7_14_day_avg, w.weekly_gross_sales, w.week_start_date, m.monthly_gross_sales, m.month_start_date FROM sales_data sd LEFT JOIN weekly_agg w ON DATE_TRUNC(\'week\', sd.order_date) = w.week_start_date AND sd.pincode = w.pincode LEFT JOIN monthly_agg m ON DATE_TRUNC(\'month\', sd.order_date) = m.month_start_date AND sd.pincode = m.pincode ORDER BY sd.order_date DESC;",
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
            