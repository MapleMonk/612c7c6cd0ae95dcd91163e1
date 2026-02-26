{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table eggozdb.maplemonk.my_sql_COCO_DATA as SELECT oc.mrp, oc.quantity * oc.mrp as MRP_VALUE, oc.quantity, oc.single_sku_rate, oc1.total_amount, oc.single_sku_rate*oc.quantity AS sku_value, oc1.discount_amount, rc.name, rc.phone_no, oc1.id, CONCAT(pp.sku_count, pp.short_name) AS sku_name, oc1.created_at, (pp.sku_count * oc.quantity) AS egg_count, CAST(TIMESTAMPADD(MINUTE, 660, oc1.created_at) AS DATE) AS Date, TIMESTAMPADD(MINUTE, 660, oc1.created_at) AS Time_stamp, bn.rate, bn.city_name FROM eggozdb.maplemonk.my_sql_order_cmsorderline oc LEFT JOIN eggozdb.maplemonk.my_sql_order_cmsorder oc1 ON oc.cms_order_id = oc1.id LEFT JOIN eggozdb.maplemonk.my_sql_product_product pp ON pp.id = oc.product_id LEFT JOIN eggozdb.maplemonk.my_sql_retailer_cmscustomer rc ON oc1.customer_id = rc.id LEFT JOIN eggozdb.maplemonk.my_sql_base_neccrates bn ON CAST(TIMESTAMPADD(MINUTE, 660, oc1.created_at) AS DATE) = bn.date AND bn.city_name = \'Barwala\' WHERE oc1.retailer_id = 16222;",
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
            