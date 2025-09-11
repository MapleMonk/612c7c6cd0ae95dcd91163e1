{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table eggozdb.maplemonk.CRM_overview as SELECT tt.*, CASE WHEN COUNT(DISTINCT DATE_TRUNC(\'MONTH\', CRM_date)) OVER (PARTITION BY Customers, code) >= 2 THEN \'Repeated\' ELSE \'Non-Repeated\' END AS status, COUNT(DISTINCT Customers) OVER (PARTITION BY first_month, code) AS first_month_customer_count, COUNT(DISTINCT DATE_TRUNC(\'MONTH\', CRM_date)) OVER (PARTITION BY Customers, code) AS months_billed, DATEDIFF( MONTH, MIN(first_month) OVER (PARTITION BY Customers, code), (SELECT MAX(DATE_TRUNC(\'MONTH\', CAST(TIMESTAMPADD(minute,660,modified_at) AS DATE))) FROM my_sql_order_cmsorder) ) + 1 AS total_months, CONCAT( ROUND( 100.0 * COUNT(DISTINCT DATE_TRUNC(\'MONTH\', CRM_date)) OVER (PARTITION BY Customers, code) / NULLIF( DATEDIFF( MONTH, MIN(first_month) OVER (PARTITION BY Customers, code), (SELECT MAX(DATE_TRUNC(\'MONTH\', CAST(TIMESTAMPADD(minute,660,modified_at) AS DATE))) FROM my_sql_order_cmsorder) ) + 1, 0 ), 2 ), \'%\' ) AS retention_ratio FROM ( SELECT oc.id AS order_id, rr.id, rr.code, CAST(TIMESTAMPADD(minute,660,oc.modified_at) AS DATE) AS CRM_date, MIN(DATE_TRUNC(\'MONTH\', CAST(TIMESTAMPADD(minute,660,oc.modified_at) AS DATE))) OVER (PARTITION BY rc.id, rr.code) AS first_month, CONCAT(rc.id,\'-\',rc.name) AS Customers, rc.loyalty_points, oc.payable_amount, ocl.single_sku_rate AS CRM_single_sku_rate, ocl.single_sku_rate*ocl.quantity AS CRM_sale, ocl.mrp, ocl.quantity AS CRM_Quantity, SUM(pp.SKU_Count*ocl.quantity) AS CRM_eggs_sold, CONCAT(pp.sku_count,pp.short_name) AS CRM_SKU, pcp.product_name AS Redeem_SKU, SUM(olrl.quantity) AS Gift_quantity, SUM(olr.point_added) AS Points_added, SUM(olr.point_subtracted) AS Points_sub, SUM(olr.point_subtracted) * 1.0/NULLIF(SUM(olr.point_added), 0) AS Redemtion_Ratio, SUM(olr.point_subtracted) OVER (PARTITION BY rc.name) * 1.0/ NULLIF(SUM(olr.point_added) OVER (PARTITION BY rc.name), 0) AS customer_Redemtion_Ratio FROM my_sql_order_cmsorder oc LEFT JOIN my_sql_retailer_retailer rr ON oc.retailer_id = rr.id LEFT JOIN my_sql_order_cmsorderline ocl ON oc.id=ocl.cms_order_id LEFT JOIN my_sql_product_product pp ON ocl.product_id=pp.id LEFT JOIN my_sql_retailer_cmscustomer rc ON oc.customer_id = rc.id LEFT JOIN my_sql_order_cmsloyaltyredeem olr ON oc.id = olr.cms_order_id AND olr.customer_id = oc.customer_id LEFT JOIN my_sql_order_cmsloyaltyredeemline olrl ON olr.id = olrl.cms_loyalty_redeem_id LEFT JOIN my_sql_product_cmsgiftproduct pcp ON olrl.gift_product_id = pcp.id GROUP BY rr.id, oc.id, rr.code, oc.modified_at, rc.id, rc.name, rc.loyalty_points, ocl.single_sku_rate, oc.payable_amount, pp.sku_count, pp.short_name, pcp.product_name, ocl.mrp, ocl.quantity, olr.point_subtracted, olr.point_added ) tt;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.MAPLEMONK.MY_SQL_order_cmsorder
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            