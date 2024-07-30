{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.plus_size_retention ( first_purchase_month DATE, m0_customers INTEGER, m0_gross_sales FLOAT, m1_customers INTEGER, m1_gross_sales FLOAT, m1_retention_rate FLOAT, m2_customers INTEGER, m2_gross_sales FLOAT, m2_retention_rate FLOAT, m3_customers INTEGER, m3_gross_sales FLOAT, m3_retention_rate FLOAT, m4_customers INTEGER, m4_gross_sales FLOAT, m4_retention_rate FLOAT, m5_customers INTEGER, m5_gross_sales FLOAT, m5_retention_rate FLOAT, m6_customers INTEGER, m6_gross_sales FLOAT, m6_retention_rate FLOAT, m7_customers INTEGER, m7_gross_sales FLOAT, m7_retention_rate FLOAT, m8_customers INTEGER, m8_gross_sales FLOAT, m8_retention_rate FLOAT, m9_customers INTEGER, m9_gross_sales FLOAT, m9_retention_rate FLOAT, m10_customers INTEGER, m10_gross_sales FLOAT, m10_retention_rate FLOAT, m11_customers INTEGER, m11_gross_sales FLOAT, m11_retention_rate FLOAT, m12_customers INTEGER, m12_gross_sales FLOAT, m12_retention_rate FLOAT ) AS ( WITH plus_size AS ( SELECT CASE WHEN (STARTSWITH(phone, \'+91\') OR STARTSWITH(phone, \'91\')) THEN RIGHT(TRIM(REPLACE(phone, \' \', \'\')), 10) ELSE TRIM(REPLACE(phone, \' \', \'\')) END AS phone_no FROM snitch_db.maplemonk.fact_items_snitch WHERE size in (\'3XL\',\'4XL\',\'5XL\',\'6XL\',\'40\',\'42\',\'44\',\'46\') AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\', \'2422140\', \'2425364\', \'2430652\', \'2422237\', \'2420623\', \'2429832\', \'2422378\', \'2428311\', \'2429064\', \'2428204\', \'2421343\', \'2431206\', \'2430491\', \'2426682\', \'2426487\', \'2426458\', \'2423575\', \'2422431\', \'2423612\', \'2426625\', \'2428117\', \'2426894\', \'2425461\', \'2426570\', \'2423455\', \'2430777\', \'2426009\', \'2428245\', \'2427269\', \'2430946\', \'2425821\', \'2429986\', \'2429085\', \'2422047\', \'2430789\', \'2420219\', \'2428341\', \'2430444\', \'2426866\', \'2431230\', \'2425839\', \'2430980\', \'2427048\', \'2430597\', \'2420499\', \'2431050\', \'2420271\', \'2426684\', \'2428747\', \'2423523\', \'2431171\', \'2430830\', \'2425325\', \'2428414\', \'2429054\', \'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') ), first_purchase AS ( SELECT CASE WHEN (STARTSWITH(phone, \'+91\') OR STARTSWITH(phone, \'91\')) THEN RIGHT(TRIM(REPLACE(phone, \' \', \'\')), 10) ELSE TRIM(REPLACE(phone, \' \', \'\')) END AS phone_no, MIN((DATE_TRUNC(\'MONTH\', order_timestamp))::DATE) AS first_purchase_month FROM snitch_db.maplemonk.fact_items_snitch WHERE LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(discount_code, \'n\')) NOT LIKE \'%influ%\' AND order_name NOT IN (\'2431093\', \'2422140\', \'2425364\', \'2430652\', \'2422237\', \'2420623\', \'2429832\', \'2422378\', \'2428311\', \'2429064\', \'2428204\', \'2421343\', \'2431206\', \'2430491\', \'2426682\', \'2426487\', \'2426458\', \'2423575\', \'2422431\', \'2423612\', \'2426625\', \'2428117\', \'2426894\', \'2425461\', \'2426570\', \'2423455\', \'2430777\', \'2426009\', \'2428245\', \'2427269\', \'2430946\', \'2425821\', \'2429986\', \'2429085\', \'2422047\', \'2430789\', \'2420219\', \'2428341\', \'2430444\', \'2426866\', \'2431230\', \'2425839\', \'2430980\', \'2427048\', \'2430597\', \'2420499\', \'2431050\', \'2420271\', \'2426684\', \'2428747\', \'2423523\', \'2431171\', \'2430830\', \'2425325\', \'2428414\', \'2429054\', \'2423596\') AND tags NOT IN (\'FLITS_LOGICERP\') GROUP BY phone_no ), customer_first_purchase as ( select * from first_purchase where phone_no in (SELECT phone_no FROM plus_size) ), trial_monthly_orders AS ( SELECT CASE WHEN (STARTSWITH(phone, \'+91\') OR STARTSWITH(phone, \'91\')) THEN RIGHT(TRIM(REPLACE(phone, \' \', \'\')), 10) ELSE TRIM(REPLACE(phone, \' \', \'\')) END AS phone_no, (DATE_TRUNC(\'MONTH\', order_timestamp))::DATE AS sale_month, COUNT(DISTINCT order_name) AS order_count, COUNT(DISTINCT phone_no) AS customer_count, SUM(gross_sales) AS gross_sales FROM snitch_db.maplemonk.fact_items_snitch WHERE phone_no IN (SELECT phone_no FROM plus_size) GROUP BY phone_no, DATE_TRUNC(\'MONTH\', order_timestamp) ), monthly_orders as ( select * from trial_monthly_orders where phone_no in (SELECT phone_no FROM plus_size) ), customer_retention AS ( SELECT cfp.phone_no, cfp.first_purchase_month, mo.sale_month, EXTRACT(YEAR FROM mo.sale_month) * 12 + EXTRACT(MONTH FROM mo.sale_month) - (EXTRACT(YEAR FROM cfp.first_purchase_month) * 12 + EXTRACT(MONTH FROM cfp.first_purchase_month)) AS month_diff, mo.gross_sales FROM customer_first_purchase cfp JOIN monthly_orders mo ON cfp.phone_no = mo.phone_no WHERE mo.sale_month >= cfp.first_purchase_month ), m0_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 0 THEN cr.phone_no END) AS m0_customers, SUM(CASE WHEN cr.month_diff = 0 THEN cr.gross_sales ELSE 0 END) AS m0_gross_sales FROM customer_retention cr GROUP BY cr.first_purchase_month ), m1_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 1 THEN cr.phone_no END) AS m1_customers, SUM(CASE WHEN cr.month_diff = 1 THEN cr.gross_sales ELSE 0 END) AS m1_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m2_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 2 THEN cr.phone_no END) AS m2_customers, SUM(CASE WHEN cr.month_diff = 2 THEN cr.gross_sales ELSE 0 END) AS m2_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m3_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 3 THEN cr.phone_no END) AS m3_customers, SUM(CASE WHEN cr.month_diff = 3 THEN cr.gross_sales ELSE 0 END) AS m3_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m4_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 4 THEN cr.phone_no END) AS m4_customers, SUM(CASE WHEN cr.month_diff = 4 THEN cr.gross_sales ELSE 0 END) AS m4_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m5_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 5 THEN cr.phone_no END) AS m5_customers, SUM(CASE WHEN cr.month_diff = 5 THEN cr.gross_sales ELSE 0 END) AS m5_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m6_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 6 THEN cr.phone_no END) AS m6_customers, SUM(CASE WHEN cr.month_diff = 6 THEN cr.gross_sales ELSE 0 END) AS m6_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m7_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 7 THEN cr.phone_no END) AS m7_customers, SUM(CASE WHEN cr.month_diff = 7 THEN cr.gross_sales ELSE 0 END) AS m7_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m8_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 8 THEN cr.phone_no END) AS m8_customers, SUM(CASE WHEN cr.month_diff = 8 THEN cr.gross_sales ELSE 0 END) AS m8_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m9_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 9 THEN cr.phone_no END) AS m9_customers, SUM(CASE WHEN cr.month_diff = 9 THEN cr.gross_sales ELSE 0 END) AS m9_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 8 ) GROUP BY cr.first_purchase_month ), m10_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 10 THEN cr.phone_no END) AS m10_customers, SUM(CASE WHEN cr.month_diff = 10 THEN cr.gross_sales ELSE 0 END) AS m10_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m11_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 11 THEN cr.phone_no END) AS m11_customers, SUM(CASE WHEN cr.month_diff = 11 THEN cr.gross_sales ELSE 0 END) AS m11_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ), m12_retention AS ( SELECT cr.first_purchase_month, COUNT(DISTINCT CASE WHEN cr.month_diff = 12 THEN cr.phone_no END) AS m12_customers, SUM(CASE WHEN cr.month_diff = 12 THEN cr.gross_sales ELSE 0 END) AS m12_gross_sales FROM customer_retention cr WHERE EXISTS ( SELECT 1 FROM customer_retention cr_prev WHERE cr_prev.phone_no = cr.phone_no AND cr_prev.month_diff = 0 ) GROUP BY cr.first_purchase_month ) SELECT m1.first_purchase_month, IFNULL(m0.m0_customers,0), m0.m0_gross_sales, IFNULL(m1.m1_customers,0), m1.m1_gross_sales, ROUND((m1.m1_customers::FLOAT /IFNULL(m0.m0_customers,0)) * 100, 2) AS m1_retention_rate, IFNULL(m2.m2_customers,0), m2.m2_gross_sales, ROUND((m2.m2_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m2_retention_rate, IFNULL(m3.m3_customers,0), m3.m3_gross_sales, ROUND((m3.m3_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m3_retention_rate, IFNULL(m4.m4_customers,0), m4.m4_gross_sales, ROUND((m4.m4_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m4_retention_rate, IFNULL(m5.m5_customers,0), m5.m5_gross_sales, ROUND((m5.m5_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m5_retention_rate, IFNULL(m6.m6_customers,0), m6.m6_gross_sales, ROUND((m6.m6_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m6_retention_rate, IFNULL(m7.m7_customers,0), m7.m7_gross_sales, ROUND((m7.m7_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m7_retention_rate, IFNULL(m8.m8_customers,0), m8.m8_gross_sales, ROUND((m8.m8_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m8_retention_rate, IFNULL(m9.m9_customers,0), m9.m9_gross_sales, ROUND((m9.m9_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m9_retention_rate, IFNULL(m10.m10_customers,0), m10.m10_gross_sales, ROUND((m10.m10_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m10_retention_rate, IFNULL(m11.m11_customers,0), m11.m11_gross_sales, ROUND((m11.m11_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m11_retention_rate, IFNULL(m12.m12_customers,0), m12.m12_gross_sales, ROUND((m12.m12_customers::FLOAT / IFNULL(m0.m0_customers,0)) * 100, 2) AS m12_retention_rate FROM m1_retention m1 LEFT JOIN m0_retention m0 ON m1.first_purchase_month = m0.first_purchase_month LEFT JOIN m2_retention m2 ON m1.first_purchase_month = m2.first_purchase_month LEFT JOIN m3_retention m3 ON m1.first_purchase_month = m3.first_purchase_month LEFT JOIN m4_retention m4 ON m1.first_purchase_month = m4.first_purchase_month LEFT JOIN m5_retention m5 ON m1.first_purchase_month = m5.first_purchase_month LEFT JOIN m6_retention m6 ON m1.first_purchase_month = m6.first_purchase_month LEFT JOIN m7_retention m7 ON m1.first_purchase_month = m7.first_purchase_month LEFT JOIN m8_retention m8 ON m1.first_purchase_month = m8.first_purchase_month LEFT JOIN m9_retention m9 ON m1.first_purchase_month = m9.first_purchase_month LEFT JOIN m10_retention m10 ON m1.first_purchase_month = m10.first_purchase_month LEFT JOIN m11_retention m11 ON m1.first_purchase_month = m11.first_purchase_month LEFT JOIN m12_retention m12 ON m1.first_purchase_month = m12.first_purchase_month ORDER BY m1.first_purchase_month DESC );",
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
            