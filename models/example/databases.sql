{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.availability_master_v2 as WITH PivotTable AS ( Select sku_group , sum(XS_units) as XS_units, sum(S_units) as S_units, sum(M_units) as M_units, sum(L_units) as L_units, sum(XL_units) as XL_units, sum(XXL_units) as XXL_units, sum(XL3_units) as XL3_units, sum(XL4_units) as XL4_units, sum(XL5_units) as XL5_units, sum(XL6_units) as XL6_units from ( SELECT sku_group, coalesce(SUM(CASE WHEN size_mapped = \'XS\' THEN units_on_hand ELSE 0 END),0) AS XS_units, coalesce(SUM(CASE WHEN size_mapped = \'S\' THEN units_on_hand ELSE 0 END),0) AS S_units, coalesce(SUM(CASE WHEN size_mapped = \'M\' THEN units_on_hand ELSE 0 END),0) AS M_units, coalesce(SUM(CASE WHEN size_mapped = \'L\' THEN units_on_hand ELSE 0 END),0) AS L_units, coalesce(SUM(CASE WHEN size_mapped = \'XL\' THEN units_on_hand ELSE 0 END),0) AS XL_units, coalesce(SUM(CASE WHEN (size_mapped = \'XXL\' OR size_mapped = \'XXl\' or size_mapped = \'2XL\') THEN units_on_hand ELSE 0 END),0) AS XXL_units, coalesce(SUM(CASE WHEN size_mapped = \'3XL\' THEN units_on_hand ELSE 0 END),0) AS XL3_units, coalesce(SUM(CASE WHEN size_mapped = \'4XL\' THEN units_on_hand ELSE 0 END),0) AS XL4_units, coalesce(SUM(CASE WHEN size_mapped = \'5XL\' THEN units_on_hand ELSE 0 END),0) AS XL5_units, coalesce(SUM(CASE WHEN size_mapped = \'6XL\' THEN units_on_hand ELSE 0 END),0) AS XL6_units FROM SNITCH_DB.MAPLEMONK.INVENTORY_AGING_BUCKETS_SNITCH GROUP BY sku_group Union ALL SELECT REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))) AS sku_group, COALESCE(SUM(CASE WHEN SIZE = \'XS\' OR SIZE = \'28\' THEN INVENTORY ELSE 0 END), 0) AS XS_units, COALESCE(SUM(CASE WHEN SIZE = \'S\' OR SIZE = \'30\' THEN INVENTORY ELSE 0 END), 0) AS S_units, COALESCE(SUM(CASE WHEN SIZE = \'M\' OR SIZE = \'32\' THEN INVENTORY ELSE 0 END), 0) AS M_units, COALESCE(SUM(CASE WHEN SIZE = \'L\' OR SIZE = \'34\' THEN INVENTORY ELSE 0 END), 0) AS L_units, COALESCE(SUM(CASE WHEN SIZE = \'XL\' OR SIZE = \'36\' THEN INVENTORY ELSE 0 END), 0) AS XL_units, COALESCE(SUM(CASE WHEN SIZE IN (\'XXL\', \'XXl\', \'2XL\', \'38\') THEN INVENTORY ELSE 0 END), 0) AS XXL_units, COALESCE(SUM(CASE WHEN SIZE = \'3XL\' THEN INVENTORY ELSE 0 END), 0) AS XL3_units, COALESCE(SUM(CASE WHEN SIZE = \'4XL\' THEN INVENTORY ELSE 0 END), 0) AS XL4_units, COALESCE(SUM(CASE WHEN SIZE = \'5XL\' THEN INVENTORY ELSE 0 END), 0) AS XL5_units, COALESCE(SUM(CASE WHEN SIZE = \'6XL\' THEN INVENTORY ELSE 0 END), 0) AS XL6_units FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = current_date GROUP BY REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))) ) where sku_group not like\'CB%\' GROUP BY SKU_GROUP ) SELECT EOQ_Test.SKU_GROUP, EOQ_Test.Product_id, EOQ_Test.price, EOQ_Test.product_name, EOQ_Test.category, EOQ_Test.final_ros, ROUND(coalesce(SUM(EOQ_Test.eoq_new),0),0) as total_eoq, coalesce(SUM(EOQ_Test.total_units_on_hand),0) as available_units, ROUND(coalesce(SUM(EOQ_Test.op),0),0) as order_point, EOQ_Test.natural_ros,EOQ_Test.sales_last_7_days,EOQ_Test.sales_last_15_days,EOQ_Test.sales_last_30_days, CASE WHEN EOQ_Test.final_ros >= 15 THEN \'1-Head\' WHEN EOQ_Test.final_ros < 15 AND EOQ_Test.final_ros >= 5 THEN \'2-Belly\' WHEN EOQ_Test.first_order_date > cast(getdate() as date)-75 THEN \'4-New\' ELSE \'3-Tail\' END AS sku_class, PivotTable.XS_units, PivotTable.S_units, PivotTable.M_units, PivotTable.L_units, PivotTable.XL_units, PivotTable.XXL_units, PivotTable.XL3_units, PivotTable.XL4_units, PivotTable.XL5_units, PivotTable.XL6_units, (CASE WHEN PivotTable.XS_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.S_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.M_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.L_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XXL_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL3_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL4_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL5_units > 0 THEN 1 ELSE 0 END) + (CASE WHEN PivotTable.XL6_units > 0 THEN 1 ELSE 0 END) AS num_size_available FROM ( SELECT a.SKU_GROUP, products.id as Product_id, products.price, product_name, category, final_ros, eoq_new, total_units_on_hand, op, first_order_date, natural_ros,sales_last_7_days,sales_last_15_days,sales_last_30_days FROM snitch_db.maplemonk.EOQ_Test a LEFT JOIN (select sku_group,id,price from (select distinct id,REVERSE(SUBSTRING(REVERSE(replace(A.value:sku,\'\"\',\'\')), CHARINDEX(\'-\', REVERSE(replace(A.value:sku,\'\"\',\'\'))) + 1)) AS SKU_GROUP,to_timestamp(replace(A.value:updated_at,\'\"\',\'\')) as UPDATED_AT, row_number ()over (partition by SKU_group order by updated_at desc) as rw, replace(A.value:price,\'\"\',\'\') as price from snitch_db.MAPLEMONK.SHOPIFY_ALL_PRODUCTS , LATERAL FLATTEN(INPUT => VARIANTS)A) where rw=1) products ON a.sku_group = products.sku_group ) AS EOQ_Test LEFT JOIN PivotTable ON EOQ_Test.SKU_GROUP = PivotTable.sku_group GROUP BY sku_class, EOQ_Test.sku_group, EOQ_Test.product_name, EOQ_Test.final_ros, EOQ_Test.category, PivotTable.XS_units, EOQ_Test.product_id, EOQ_Test.Price, PivotTable.S_units, PivotTable.M_units, PivotTable.L_units, PivotTable.XXL_units, PivotTable.XL_units, PivotTable.XL3_units, PivotTable.XL4_units, PivotTable.XL5_units, PivotTable.XL6_units, EOQ_Test.natural_ros,EOQ_Test.sales_last_7_days,EOQ_Test.sales_last_15_days,EOQ_Test.sales_last_30_days ORDER BY sku_class, EOQ_Test.final_ros DESC;",
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
                        