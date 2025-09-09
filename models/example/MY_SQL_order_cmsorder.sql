{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table eggozdb.maplemonk.CRM_overview as select tt.*, CASE WHEN COUNT(DISTINCT DATE_TRUNC(\'MONTH\', CRM_date)) OVER (PARTITION BY Customers,code) >= 2 THEN \'Repeated\' ELSE \'Non-Repeated\' END AS status from ( select oc.id as order_id, rr.id, rr.code, cast(timestampadd(minute,660,oc.modified_at) as date) as CRM_date, concat(rc.id,\'-\',rc.name) as Customers, rc.loyalty_points, oc.payable_amount, ocl.single_sku_rate as CRM_single_sku_rate, ocl.single_sku_rate*ocl.quantity as CRM_sale, ocl.mrp, ocl.quantity as CRM_Quantity, sum(pp.SKU_Count*ocl.quantity) as CRM_eggs_sold, concat(pp.sku_count,pp.short_name) as CRM_SKU, pcp.product_name as Redeem_SKU, sum(olrl.quantity) as Gift_quantity, sum(olr.point_added) as Points_added, sum(olr.point_subtracted) as Points_sub, SUM(olr.point_subtracted) * 1.0/NULLIF(SUM(olr.point_added), 0) as Redemtion_Ratio, SUM(olr.point_subtracted) OVER (PARTITION BY rc.name) * 1.0/ NULLIF(SUM(olr.point_added) OVER (PARTITION BY rc.name), 0) AS customer_Redemtion_Ratio from my_sql_order_cmsorder oc left join my_sql_retailer_retailer rr on oc.retailer_id = rr.id left join my_sql_order_cmsorderline ocl on oc.id=ocl.cms_order_id left join my_sql_product_product pp on ocl.product_id=pp.id left join my_sql_retailer_cmscustomer rc on oc.customer_id = rc.id left join my_sql_order_cmsloyaltyredeem olr on oc.id = olr.cms_order_id and olr.customer_id = oc.customer_id left join my_sql_order_cmsloyaltyredeemline olrl on olr.id = olrl.cms_loyalty_redeem_id left join my_sql_product_cmsgiftproduct pcp on olrl.gift_product_id = pcp.id group by rr.id, oc.id, rr.code, oc.modified_at, rc.id, rc.name, rc.loyalty_points, rc.id, ocl.single_sku_rate, oc.payable_amount, pp.sku_count, pp.short_name, pcp.product_name, ocl.mrp, ocl.quantity, olr.point_subtracted, olr.point_added ) tt; create or replace table eggozdb.maplemonk.CRM_Primary_secondary as select cm.*,ps.* from eggozdb.maplemonk.CRM_overview cm left join primary_and_secondary_sku ps on cm.code = ps.retailer_name and cm.crm_sku = ps.sku;",
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
            