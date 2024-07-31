{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.category_visibility as ( with cat_sales as ( select order_timestamp::date as date, case when category like \'Shirt%\' then \'Shirts\' when category = \'Denim\' then \'Jeans\' else category end as new_category, sum(gross_sales) as total_sales, sum(net_sales) as net_sales, sum(quantity) as total_quant, sum(quantity-rto_quant-dto_quant-cancel_quant) as net_quant, count(distinct order_name) as total_orders from snitch_db.maplemonk.fact_items_snitch where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') group by 1,2 ), cat_inventory as ( select case when category like \'Shirt%\' then \'Shirts\' when category = \'Denim\' then \'Jeans\' else category end as new_category, sum(available_units) as total_quant_available, sum(S_UNITS) as S_UNITS, sum(M_UNITS) as M_UNITS, sum(L_UNITS) as L_UNITS, from snitch_db.maplemonk.availability_master_v2 where status = \'active\' and sku_class not in (\'Not-Cataloged\',\'Draft\') group by 1 ) select a.*,b.total_quant_available,b.S_UNITS,b.M_UNITS,b.L_UNITS from cat_sales a left join cat_inventory b on a.new_category = b.new_category );",
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
            