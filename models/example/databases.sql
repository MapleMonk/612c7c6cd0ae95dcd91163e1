{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.category_visibility as ( with cat_sales as ( select order_timestamp::date as date, sku_group, case when category like \'Shirt%\' then \'Shirts\' when category = \'Denim\' then \'Jeans\' else category end as new_category, sum(gross_sales) as total_sales, sum(quantity) as total_quant from snitch_db.maplemonk.fact_items_snitch where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') group by 1,2,3 ), inventory as ( select date, REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))) AS sku_group, sum(inventory) as sellable_inventory from snitch_db.maplemonk.snitch_final_inventory_wh2 group by 1,2 ), inventory_status as ( select sku_group, sku_class from snitch_db.maplemonk.availability_master_v2 ), main_data as ( select a.*, b.sku_class, from inventory a left join inventory_status b on lower(a.sku_group) = lower(b.sku_group) ) select a.*,b.new_category,b.total_sales,b.total_quant from main_data a left join cat_sales b on a.sku_group = b.sku_group and a.date = b.date );",
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
            