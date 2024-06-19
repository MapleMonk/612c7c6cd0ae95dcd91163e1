{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.store_replen_2 as WITH sold_yesterday as ( select sku_group,sum(suborder_quantity) as units_sold_store, branch_code, order_date from snitch_db.maplemonk.STORE_fact_items_offline where order_date = current_date - 1 and sku_group not like \'%CB\' group by sku_group,branch_code, order_date ), thresholds as ( select * from snitch_db.maplemonk.cutsizereplen ), store_replen_interim as ( select store_replen_1.*, omc.percentage_category, (CASE WHEN XS_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN S_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN M_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN L_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN XL_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN XXL_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN XL3_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN XL4_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN XL5_UNITS > 1 THEN 1 ELSE 0 END) + (CASE WHEN XL6_UNITS > 1 THEN 1 ELSE 0 END) AS NUM_SIZE_WH_NEW, CASE WHEN round(sum_size_ratio_percentage_by_sku_group,0) <100 THEN \'REPLEN\' ELSE \'DONT REPLEN\' END AS REPLEN_FLAG, (CASE WHEN XS_UNITS > 1 AND XS_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN S_UNITS > 1 AND S_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN M_UNITS > 1 AND M_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN L_UNITS > 1 AND L_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN XL_UNITS > 1 AND XL_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN XXL_UNITS > 1 AND XXL_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN XL3_UNITS > 1 AND XL3_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN XL4_UNITS > 1 AND XL4_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN XL5_UNITS > 1 AND XL5_UNITS_STORE>0 THEN 1 ELSE 0 END) + (CASE WHEN XL6_UNITS > 1 AND XL6_UNITS_STORE>0 THEN 1 ELSE 0 END) AS NUM_SAME_SIZES, NUM_SIZE_WH_NEW + NUM_SIZE_AVAILABLE - NUM_SAME_SIZES as SIZE_AFTER_REPLEN, CASE WHEN (SIZE_AFTER_REPLEN > NUM_SIZE_AVAILABLE AND REPLEN_FLAG=\'REPLEN\') THEN 1 ELSE 0 END AS FINAL_ALLOC, CASE WHEN (FINAL_ALLOC=0 AND REPLEN_FLAG=\'REPLEN\' AND sum_size_ratio_percentage_by_sku_group < 55) THEN \'OUTWARD\' WHEN (FINAL_ALLOC=0 AND REPLEN_FLAG=\'REPLEN\' AND sum_size_ratio_percentage_by_sku_group >= 55) THEN \'CANNOT REPLEN\' ELSE REPLEN_FLAG END AS Final_action, omc.percentage_category as sku_offline_pareto from snitch_db.maplemonk.store_replen_1 LEFT JOIN snitch_db.maplemonk.offline_master_core omc ON store_replen_1.sku_group=omc.sku_group_final order by priority,units_on_hand desc ) select sri.*, sy.order_date as yesterday_order_date, thresholds.super_category_tsh, thresholds.xs_tsh_low, thresholds.s_tsh_low, thresholds.m_tsh_low, thresholds.l_tsh_low, thresholds.xl_tsh_low, thresholds.xxl_tsh_low, thresholds.xl3_tsh_low, thresholds.xl4_tsh_low, thresholds.xl5_tsh_low, thresholds.xl6_tsh_low, thresholds.xl7_tsh_low, thresholds.xl8_tsh_low, thresholds.s_tsh_high, thresholds.m_tsh_high, thresholds.l_tsh_high, thresholds.xl_tsh_high, thresholds.xxl_tsh_high, thresholds.xl3_tsh_high, thresholds.xl4_tsh_high, thresholds.xl5_tsh_high, thresholds.xl6_tsh_high, thresholds.xl7_tsh_high, thresholds.xl8_tsh_high, CAST(LEFT(percentage_category, LEN(percentage_category) - 1) AS INT) as pareto, CASE WHEN sy.units_sold_store IS NULL THEN 0 ELSE sy.units_sold_store END as units_sold_yesterday, CASE WHEN (XS_UNITS > 1 AND XS_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XS_UNITS,xs_tsh_high) WHEN (XS_UNITS > 1 AND XS_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XS_UNITS,xs_tsh_low) ELSE 0 END AS XS_alloc, CASE WHEN (S_UNITS > 1 AND S_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(S_UNITS,S_tsh_high) WHEN (S_UNITS > 1 AND S_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(S_UNITS,s_tsh_low) ELSE 0 END AS S_alloc, CASE WHEN (M_UNITS > 1 AND M_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(M_UNITS,M_tsh_high) WHEN (M_UNITS > 1 AND M_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(M_UNITS,M_tsh_low) ELSE 0 END AS M_alloc, CASE WHEN (L_UNITS > 1 AND L_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(L_UNITS,L_tsh_high) WHEN (L_UNITS > 1 AND L_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(L_UNITS,L_tsh_low) ELSE 0 END AS L_alloc, CASE WHEN (XL_UNITS > 1 AND XL_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XL_UNITS,XL_tsh_high) WHEN (XL_UNITS > 1 AND XL_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XL_UNITS,XL_tsh_low) ELSE 0 END AS XL_alloc, CASE WHEN (XXL_UNITS > 1 AND XXL_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XXL_UNITS,XXL_tsh_high) WHEN (XXL_UNITS > 1 AND XXL_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XXL_UNITS,XXL_tsh_low) ELSE 0 END AS XXL_alloc, CASE WHEN (XL3_UNITS > 1 AND XL3_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XL3_UNITS,XL3_tsh_high) WHEN (XL3_UNITS > 1 AND XL3_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XL3_UNITS,XL3_tsh_low) ELSE 0 END AS XL3_alloc, CASE WHEN (XL4_UNITS > 1 AND XL4_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XL4_UNITS,XL4_tsh_high) WHEN (XL4_UNITS > 1 AND XL4_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XL4_UNITS,XL4_tsh_low) ELSE 0 END AS XL4_alloc, CASE WHEN (XL5_UNITS > 1 AND XL5_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XL5_UNITS,XL5_tsh_high) WHEN (XL5_UNITS > 1 AND XL5_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XL5_UNITS,XL5_tsh_low) ELSE 0 END AS XL5_alloc, CASE WHEN (XL6_UNITS > 1 AND XL6_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto <=40) THEN LEAST(XL6_UNITS,XL3_tsh_high) WHEN (XL6_UNITS > 1 AND XL6_UNITS_STORE=0 AND FINAL_ALLOC=1 AND pareto >40) THEN LEAST(XL6_UNITS,XL6_tsh_low) ELSE 0 END AS XL6_alloc FROM store_replen_interim sri LEFT JOIN sold_yesterday sy ON sri.sku_group=sy.sku_group LEFT JOIN thresholds ON sri.availability_category=thresholds.category_tsh ORDER BY priority,units_on_hand desc",
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
                        