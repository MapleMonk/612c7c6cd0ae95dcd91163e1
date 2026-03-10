{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table eggozdb.maplemonk.my_sql_COCO_DATA AS WITH customer_first_visit AS ( SELECT rc.phone_no, MIN(DATEADD(MINUTE,660,oc1.created_at)::DATE) AS first_visit_date FROM eggozdb.maplemonk.my_sql_order_cmsorderline oc LEFT JOIN eggozdb.maplemonk.my_sql_order_cmsorder oc1 ON oc.cms_order_id = oc1.id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_cmscustomer rc ON oc1.customer_id = rc.id WHERE oc1.retailer_id = 16222 GROUP BY rc.phone_no ), retention_calc AS ( SELECT cf.phone_no, MONTHNAME(cf.first_visit_date) AS first_month, CASE WHEN COUNT(o.id) > 0 THEN 100 ELSE 0 END AS retention_rate FROM customer_first_visit cf LEFT JOIN eggozdb.maplemonk.my_sql_order_cmsorder o ON 1=1 LEFT JOIN eggozdb.maplemonk.my_sql_retailer_cmscustomer rc ON o.customer_id = rc.id AND rc.phone_no = cf.phone_no AND DATE_TRUNC(\'MONTH\', DATEADD(MINUTE,660,o.created_at)) = DATEADD(MONTH,1,DATE_TRUNC(\'MONTH\',cf.first_visit_date)) GROUP BY cf.phone_no, cf.first_visit_date ) SELECT oc.mrp, oc.quantity * oc.mrp AS MRP_VALUE, oc.quantity, oc.single_sku_rate, oc1.total_amount, oc.single_sku_rate * oc.quantity AS sku_value, oc1.discount_amount, rc.name, rc.phone_no, oc1.id, CONCAT(pp.sku_count, pp.short_name) AS sku_name, oc1.created_at, (pp.sku_count * oc.quantity) AS egg_count, DATEADD(MINUTE,660,oc1.created_at)::DATE AS Date, DATEADD(MINUTE,660,oc1.created_at) AS Time_stamp, bn.rate, bn.city_name, r.first_month, r.retention_rate, CASE WHEN (oc.single_sku_rate * oc.quantity) = 0 THEN \'Promo\' ELSE \'Sales\' END AS order_type FROM eggozdb.maplemonk.my_sql_order_cmsorderline oc LEFT JOIN eggozdb.maplemonk.my_sql_order_cmsorder oc1 ON oc.cms_order_id = oc1.id LEFT JOIN eggozdb.maplemonk.my_sql_product_product pp ON pp.id = oc.product_id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_cmscustomer rc ON oc1.customer_id = rc.id LEFT JOIN retention_calc r ON rc.phone_no = r.phone_no LEFT JOIN eggozdb.maplemonk.my_sql_base_neccrates bn ON DATEADD(MINUTE,660,oc1.created_at)::DATE = bn.date AND bn.city_name = \'Barwala\' WHERE oc1.retailer_id = 16222;",
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
            