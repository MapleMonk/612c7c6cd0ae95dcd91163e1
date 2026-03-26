{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.my_sql_COCO_DATA AS WITH base_orders AS ( SELECT oc1.customer_id, rc.phone_no, DATE_TRUNC(\'MONTH\', DATEADD(MINUTE,660,oc1.created_at)) AS order_month FROM eggozdb.maplemonk.my_sql_order_cmsorder oc1 LEFT JOIN eggozdb.maplemonk.my_sql_retailer_cmscustomer rc ON oc1.customer_id = rc.id WHERE oc1.retailer_id = 16222 ), customer_cohort AS ( SELECT customer_id, phone_no, MIN(order_month) AS cohort_month FROM base_orders GROUP BY customer_id, phone_no ), cohort_size AS ( SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_customers FROM customer_cohort GROUP BY cohort_month ), retention_calc AS ( SELECT cc.cohort_month, DATEDIFF(MONTH, cc.cohort_month, bo.order_month) AS month_number, COUNT(DISTINCT cc.customer_id) AS retained_customers FROM customer_cohort cc LEFT JOIN base_orders bo ON cc.customer_id = bo.customer_id GROUP BY cc.cohort_month, month_number ), final_retention AS ( SELECT r.cohort_month, r.month_number, r.retained_customers, cs.cohort_customers, ROUND(r.retained_customers * 100.0 / cs.cohort_customers, 2) AS retention_rate FROM retention_calc r LEFT JOIN cohort_size cs ON r.cohort_month = cs.cohort_month ) SELECT oc1.customer_id, oc.mrp, oc.quantity * oc.mrp AS MRP_VALUE, oc.quantity, oc.single_sku_rate, oc1.total_amount, oc.single_sku_rate * oc.quantity AS sku_value, oc1.discount_amount, rc.name, rc.phone_no, oc1.id, CONCAT(pp.sku_count, pp.short_name) AS sku_name, oc1.created_at, (pp.sku_count * oc.quantity) AS egg_count, DATEADD(MINUTE,660,oc1.created_at)::DATE AS Date, DATEADD(MINUTE,660,oc1.created_at) AS Time_stamp, bn.rate, bn.city_name, fr.cohort_month, fr.month_number, fr.retained_customers, fr.cohort_customers, fr.retention_rate, CASE WHEN (oc.single_sku_rate * oc.quantity) = 0 THEN \'Promo\' ELSE \'Sales\' END AS order_type FROM eggozdb.maplemonk.my_sql_order_cmsorderline oc LEFT JOIN eggozdb.maplemonk.my_sql_order_cmsorder oc1 ON oc.cms_order_id = oc1.id LEFT JOIN eggozdb.maplemonk.my_sql_product_product pp ON pp.id = oc.product_id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_cmscustomer rc ON oc1.customer_id = rc.id LEFT JOIN customer_cohort cc ON oc1.customer_id = cc.customer_id LEFT JOIN final_retention fr ON cc.cohort_month = fr.cohort_month AND DATEDIFF( MONTH, cc.cohort_month, DATE_TRUNC(\'MONTH\', DATEADD(MINUTE,660,oc1.created_at)) ) = fr.month_number LEFT JOIN eggozdb.maplemonk.my_sql_base_neccrates bn ON DATEADD(MINUTE,660,oc1.created_at)::DATE = bn.date AND bn.city_name = \'Barwala\' WHERE oc1.retailer_id = 16222;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            