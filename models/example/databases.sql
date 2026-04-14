{{ config(
            materialized='table',
                post_hook={
                    "sql": "INSERT INTO SNITCH_DB.MAPLEMONK.SPEED_INVENTORY AS with data as ( select CASE WHEN BRANCH_CODE = 23 then \'TRIBECA\' WHEN BRANCH_CODE = 96 then \'COLABA\' WHEN BRANCH_CODE = 57 then \'BANASHANKARI\' WHEN BRANCH_CODE = 38 then \'BANER\' WHEN BRANCH_CODE = 26 then \'LINKING ROAD\' WHEN BRANCH_CODE = 14 then \'BEL ROAD\' WHEN BRANCH_CODE = 142 then \'HINJEWADI\' WHEN BRANCH_CODE = 68 then \'DAHISAR\' else null end as store, branch_code, sku, inventory_atp::INTEGER as atp, inventory_total::INTEGER as total, sku_inventory_buffer::INTEGER as sku_buffer, location_inventory_buffer::INTEGER as location_buffer, location_visible, enabled, location, query_hour_ist::DATE as saved_date from snitch_db.maplemonk.s3_product_inventory where branch_code in (\'96\', \'68\', \'57\', \'38\', \'26\', \'23\', \'142\', \'14\') and sku not like \'8%\' and sku not like \'%CB%\' and sku not like \'%HANG%\' and sku not like \'%UNI%\' and sku not like \'%NT%\' and sku not like \'%GIFT%\' ) , buffer as ( select branch_code, sku, MAX(sku_buffer) as buffer from data where location_visible = true group by 1,2 order by 3 desc ), correct_inv as ( select branch_code, location, location_visible, sku, sum(total) as total, from data group by 1,2,3,4 ), buffermaths as ( select c.* , coalesce(b.buffer,0) as buffer, CASE WHEN Location NOT LIKE \'%FRONT%\' THEN (c.total - GREATEST(c.total - b.buffer, 0)) ELSE 0 END AS actual_buffer, CASE WHEN Location NOT LIKE \'%FRONT%\' THEN GREATEST(c.total - coalesce(b.buffer,0), 0) when location like \'%FRONT%\' then 0 ELSE GREATEST(c.total, 0) END AS atp from correct_inv c left join buffer b on CONCAT(c.branch_code,c.sku) = CONCAT(b.branch_code,b.sku) ), uc as (select sku, category_code, skugroup, shopify_status, sku_class, sales_last_7_days, sales_last_15_days, sales_last_30_days, natural_ros from snitch_db.maplemonk.uc_final_item_master) select CASE WHEN BRANCH_CODE = 23 then \'TRIBECA\' WHEN BRANCH_CODE = 96 then \'COLABA\' WHEN BRANCH_CODE = 57 then \'BANASHANKARI\' WHEN BRANCH_CODE = 38 then \'BANER\' WHEN BRANCH_CODE = 26 then \'LINKING ROAD\' WHEN BRANCH_CODE = 14 then \'BEL ROAD\' WHEN BRANCH_CODE = 142 then \'HINJEWADI\' WHEN BRANCH_CODE = 68 then \'DAHISAR\' else null end as store, m.*, uc.category_code, uc.skugroup, uc.shopify_status, uc.sku_class, uc.sales_last_7_days, uc.sales_last_15_days, uc.sales_last_30_days, uc.natural_ros, current_date as saved_date from buffermaths m left join uc on m.sku = uc.sku",
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
            