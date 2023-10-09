{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.availability_master as WITH PivotTable AS ( SELECT sku_group, coalesce(SUM(CASE WHEN size_mapped = \'XS\' THEN units_on_hand ELSE 0 END),0) AS XS_units, coalesce(SUM(CASE WHEN size_mapped = \'S\' THEN units_on_hand ELSE 0 END),0) AS S_units, coalesce(SUM(CASE WHEN size_mapped = \'M\' THEN units_on_hand ELSE 0 END),0) AS M_units, coalesce(SUM(CASE WHEN size_mapped = \'L\' THEN units_on_hand ELSE 0 END),0) AS L_units, coalesce(SUM(CASE WHEN size_mapped = \'XL\' THEN units_on_hand ELSE 0 END),0) AS XL_units, coalesce(SUM(CASE WHEN (size_mapped = \'XXL\' OR size_mapped = \'XXl\' or size_mapped = \'2XL\') THEN units_on_hand ELSE 0 END),0) AS XXL_units, coalesce(SUM(CASE WHEN size_mapped = \'3XL\' THEN units_on_hand ELSE 0 END),0) AS XL3_units, coalesce(SUM(CASE WHEN size_mapped = \'4XL\' THEN units_on_hand ELSE 0 END),0) AS XL4_units, coalesce(SUM(CASE WHEN size_mapped = \'5XL\' THEN units_on_hand ELSE 0 END),0) AS XL5_units, coalesce(SUM(CASE WHEN size_mapped = \'6XL\' THEN units_on_hand ELSE 0 END),0) AS XL6_units FROM SNITCH_DB.MAPLEMONK.INVENTORY_AGING_BUCKETS_SNITCH GROUP BY sku_group ) SELECT EOQ.SKU_GROUP, EOQ.Product_id, EOQ.price, EOQ.product_name, EOQ.category, EOQ.final_ros, ROUND(coalesce(SUM(EOQ.eoq_new),0),0) as total_eoq, coalesce(SUM(EOQ.total_units_on_hand),0) as available_units, ROUND(coalesce(SUM(EOQ.op),0),0) as order_point, EOQ.natural_ros,EOQ.sales_last_7_days,EOQ.sales_last_15_days,EOQ.sales_last_30_days, CASE WHEN EOQ.final_ros >= 15 THEN \'1-Head\' WHEN EOQ.final_ros < 15 AND EOQ.final_ros >= 5 THEN \'2-Belly\' WHEN EOQ.first_order_date > cast(getdate() as date)-75 THEN \'4-New\' ELSE \'3-Tail\' END AS sku_class, PivotTable.XS_units, PivotTable.S_units, PivotTable.M_units, PivotTable.L_units, PivotTable.XL_units, PivotTable.XXL_units, PivotTable.XL3_units, PivotTable.XL4_units, PivotTable.XL5_units, PivotTable.XL6_units, (CASE WHEN PivotTable.XS_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.S_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.M_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.L_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XXL_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL3_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL4_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL5_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL6_units > 0 THEN 1 ELSE 0 END) AS num_size_available FROM ( SELECT a.SKU_GROUP, products.id as Product_id, products.price, product_name, category, final_ros, eoq_new, total_units_on_hand, op, first_order_date, natural_ros,sales_last_7_days,sales_last_15_days,sales_last_30_days FROM snitch_db.maplemonk.eoq a LEFT JOIN (select sku_group,id,price from (select distinct id,REVERSE(SUBSTRING(REVERSE(replace(A.value:sku,\'\"\',\'\')), CHARINDEX(\'-\', REVERSE(replace(A.value:sku,\'\"\',\'\'))) + 1)) AS SKU_GROUP,to_timestamp(replace(A.value:updated_at,\'\"\',\'\')) as UPDATED_AT, row_number ()over (partition by SKU_group order by updated_at desc) as rw, replace(A.value:price,\'\"\',\'\') as price from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS , LATERAL FLATTEN(INPUT => VARIANTS)A) where rw=1) products ON a.sku_group = products.sku_group ) AS EOQ LEFT JOIN PivotTable ON EOQ.SKU_GROUP = PivotTable.sku_group GROUP BY sku_class, EOQ.sku_group, EOQ.product_name, EOQ.final_ros, EOQ.category, PivotTable.XS_units, EOQ.product_id, EOQ.Price, PivotTable.S_units, PivotTable.M_units, PivotTable.L_units, PivotTable.XXL_units, PivotTable.XL_units, PivotTable.XL3_units, PivotTable.XL4_units, PivotTable.XL5_units, PivotTable.XL6_units, EOQ.natural_ros,EOQ.sales_last_7_days,EOQ.sales_last_15_days,EOQ.sales_last_30_days ORDER BY sku_class, EOQ.final_ros DESC",
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
                        