{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.store_replen_3 as Select BRANCH_CODE, BRANCH_NAME, SKU_GROUP, PRIORITY, FINAL_ACTION, PRODUCT_NAME, AVAILABILITY_CATEGORY, ifnull(REPLEN_UNITS,0) - ifnull(qty,0) as REPLEN_UNITS, REPLEN_SIZE, SKU_MAP_REPLEN, sku_code, PARETO, SKU_CLASS, CUML_REPLEN_UNITS From ( With Overall as( Select * From ( with replen_db as ( select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xs_alloc as replen_units, \'XS\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xs_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, s_alloc as replen_units, \'S\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where s_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, m_alloc as replen_units, \'M\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where m_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, l_alloc as replen_units, \'L\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where l_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xl_alloc as replen_units, \'XL\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xl_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xxl_alloc as replen_units, \'XXL\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xxl_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xl3_alloc as replen_units, \'3XL\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xl3_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xl4_alloc as replen_units, \'4XL\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xl4_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xl5_alloc as replen_units, \'5XL\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xl5_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class UNION select branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, xl6_alloc as replen_units, \'6XL\' as replen_size, concat(sku_group,\'-\',replen_size) as sku_map_replen, pareto, sku_class from snitch_db.maplemonk.store_replen_2 where xl6_alloc>0 group by branch_code, branch_name, sku_group, priority, final_action, product_name, availability_category, replen_units, sku_map_replen, pareto, sku_class order by priority, pareto, replen_units desc ), sku_map as ( select sku_code, REVERSE(SUBSTRING(REVERSE(sku_code), 1, POSITION(\'-\', REVERSE(sku_code)) - 1)) AS size, CASE WHEN size=\'28\' THEN \'XS\' WHEN size=\'30\' THEN \'S\' WHEN size=\'32\' THEN \'M\' WHEN size=\'34\' THEN \'L\' WHEN size=\'36\' THEN \'XL\' WHEN size=\'38\' THEN \'XXL\' WHEN size=\'40\' THEN \'3XL\' WHEN size=\'42\' THEN \'4XL\' WHEN size=\'44\' THEN \'5XL\' WHEN size=\'46\' THEN \'6XL\' WHEN size=\'48\' THEN \'7XL\' WHEN size=\'50\' THEN \'8XL\' WHEN size=\'\' THEN \'NA\' WHEN size IS NULL THEN \'NA\' ELSE size END AS size_map, REVERSE(SUBSTRING(REVERSE(sku_code), CHARINDEX(\'-\', REVERSE(sku_code)) + 1)) AS sku_group, concat(sku_group,\'-\',size_map) as sku_map from ( select sku as sku_code from snitch_db.maplemonk.inventory_aging_buckets_snitch union select logicusercode as sku_code from snitch_db.maplemonk.logicerp23_24_get_stock_in_hand union select sku as sku_code from snitch_db.maplemonk.STORE_fact_items_offline union select sku as sku_code from snitch_db.snitch.product_dim group by sku_code ) ) select replen_db.*, sku_map.sku_code, SUM(replen_units) OVER (PARTITION BY sku_code ORDER BY sku_code,priority,pareto) AS cuml_replen_units from replen_db left join sku_map on replen_db.sku_map_replen=sku_map.sku_map order by sku_code,priority, pareto ) ), Jit AS ( SELECT \"ITEM CODE\", BRANCH_CODE, STORE, SUM(qty) AS qty FROM SNITCH_DB.MAPLEMONK.JIT_OFFLINE_GOODS GROUP BY 1, 2, 3 ) SELECT a.*, b.qty FROM Overall a LEFT JOIN Jit b ON a.SKU_CODE = b.\"ITEM CODE\" AND a.BRANCH_CODE = b.BRANCH_CODE ) where ifnull(REPLEN_UNITS,0) - ifnull(qty,0) >0 ORDER BY SKU_CODE, PRIORITY, PARETO;",
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
            