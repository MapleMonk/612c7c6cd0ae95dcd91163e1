{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.design_fact_items_size_level as ( WITH live_date AS ( SELECT min(a.published_at::date) AS live_date, b.sku_group FROM snitch_db.maplemonk.shopifyindia_new_products a LEFT JOIN snitch_db.maplemonk.availability_master_v2 b ON a.id = b.id where a.published_at::date is not null group by 2 ), first_order_date AS ( SELECT sku_group, MIN(order_timestamp::date) AS first_order_date FROM snitch_db.maplemonk.fact_items_snitch GROUP BY 1 ), final_live_date AS ( SELECT a.sku_group, CASE WHEN a.live_date <= b.first_order_date THEN a.live_date ELSE b.first_order_date END AS live_date FROM live_date a LEFT JOIN first_order_date b ON a.sku_group = b.sku_group ), sales_data AS ( SELECT a.order_timestamp::date as date, a.sku_group, a.size, SUM(a.quantity) AS sales_dod FROM snitch_db.maplemonk.fact_items_snitch a LEFT JOIN final_live_date b ON a.sku_group = b.sku_group WHERE LOWER(IFNULL(a.discount_code, \'n\')) NOT LIKE \'%eco%\' AND LOWER(IFNULL(a.discount_code, \'n\')) NOT LIKE \'%influ%\' AND a.order_name NOT IN (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') AND a.tags NOT IN (\'FLITS_LOGICERP\') and discount_code != \'CNT_4HHD3430RA9W\' GROUP BY 1, 2, 3 UNION SELECT a.order_date as date, a.sku_group, a.size, SUM(a.shipping_quantity) AS sales_dod FROM snitch_db.maplemonk.STORE_fact_items_offline a LEFT JOIN final_live_date b ON a.sku_group = b.sku_group WHERE a.marketplace_mapped NOT LIKE \'%WH%\' GROUP BY 1, 2, 3 UNION SELECT a.order_date as date, a.sku_group, SPLIT_PART(a.sku, \'-\', -1) AS size, SUM(a.shipping_quantity) AS sales_dod FROM snitch_db.maplemonk.unicommerce_fact_items_snitch a LEFT JOIN final_live_date b ON a.sku_group = b.sku_group WHERE a.marketplace_mapped IN (\'Myntra\', \'MYNTRA\', \'FLIPKART\', \'AMAZON\', \'AJIO\') GROUP BY 1, 2, 3 ), sales_data_final as ( select date, sku_group, size, sum(sales_dod) as sales_dod from sales_data group by 1,2,3 ), final_sales_data AS ( SELECT sales.sku_group, sales.size, ifnull(SUM(case when sales.date <= DATEADD(day, 30, live.live_date) then sales_dod end),0) AS first_30d_sales, ifnull(SUM(case when sales.date >= DATEADD(day, 30, live.live_date) and sales.date <= DATEADD(day, 60, live.live_date) then sales_dod end),0) AS second_30d_sales, ifnull(SUM(case when sales.date >= DATEADD(day, -30, current_date) then sales_dod end),0) AS last_30d_sales, ifnull(sum(case when sales.date != current_date then sales_dod end),0) overall_sales FROM sales_data_final sales left join final_live_date live on sales.sku_group = live.sku_group GROUP BY 1, 2 ), dod_inventory as ( select count(distinct \"Item Code\") as inventory, date_trunc(\'week\',\"Item Created On\"::date) as date, REVERSE(SUBSTRING(REVERSE(\"Vendor skuCode\"), CHARINDEX(\'-\', REVERSE(\"Vendor skuCode\")) + 1, LEN(\"Vendor skuCode\"))) AS sku_group, SPLIT_PART(\"Vendor skuCode\", \'-\', -1) AS size, from snitch_db.maplemonk.unicommerce_itembarcode_report group by 2,3,4 union all select sum(\"Quantity Received\") as inventory, date_trunc(\'week\',\"GRN Date\"::date) as date, REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))) AS sku_group, SPLIT_PART(\"Item SkuCode\", \'-\', -1) AS size, from snitch_db.maplemonk.unicommerce_final_get_grn_report group by 2,3,4 ), starting_inventory as ( select a.sku_group, a.size, sum(a.inventory) as inventory, from dod_inventory a left join final_live_date b on a.sku_group = b.sku_group where a.date <= dateadd(day,60,b.live_date) group by 1,2 ), cumulative_sales as ( SELECT sku_group, size, date, sales_dod, SUM(sales_dod) OVER (PARTITION BY sku_group,size ORDER BY date) AS cumulative_sales FROM sales_data_final ORDER BY sku_group, date, size ), days_to_reach_target AS ( select *,datediff(day,live_date,date) + 1 as sold_out_in from ( SELECT cum.sku_group, cum.size, cum.date, live.live_date, cum.cumulative_sales, ROW_NUMBER() OVER (PARTITION BY cum.sku_group,cum.size ORDER BY cum.date) AS day_number FROM cumulative_sales cum left join starting_inventory inv on cum.sku_group = inv.sku_group and cum.size = inv.size left join final_live_date live on cum.sku_group = live.sku_group WHERE cumulative_sales >= (inv.inventory - 5) ) where day_number = 1 ), size_count as ( select sku_group,count(size) as size_count from starting_inventory group by 1 ), clicks as ( select a.sku_group, round(div0(ifnull(sum(a.clicks),0),sum(c.size_count)),0) as first_30d_clicks from snitch_db.maplemonk.clicks_itemid a left join final_live_date b on a.sku_group = b.sku_group left join size_count c on a.sku_group = c.sku_group where a.ga_date <= dateadd(day,30,b.live_date) group by 1 ), returns_data as ( select a.sku_group, a.size, sum(a.return_qty) as first_30d_returns from (select order_date as date, REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1, LEN(sku))) AS sku_group, SPLIT_PART(sku, \'-\', -1) AS size, round(ifnull(sum(return_qty),0),0) as return_qty from snitch_db.maplemonk.return_fact_items group by 1,2,3 ) a left join final_live_date b on a.sku_group = b.sku_group where a.date <= dateadd(day,30,b.live_date) group by 1,2 ), metafields as ( select sku_group, product_type, price, occassion, print_design, collar, material, sleeve_type, fit, style, color, closure, length, designs, from snitch_db.maplemonk.metafields_data ), customers_data as ( select sku_group,size, customer_id from snitch_db.maplemonk.fact_items_snitch where lower(new_customer_flag) = \'new\' and lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and discount_code != \'CNT_4HHD3430RA9W\' ), acq_customers as ( select sku_group, size, count(distinct customer_id) as acq_customers from customers_data group by 1,2 ), customer_purchases as ( select customer_id, count(distinct order_name) as gross_orders from snitch_db.maplemonk.fact_items_snitch where lower(ifnull(discount_code,\'n\')) not like \'%eco%\' and lower(ifnull(discount_code,\'n\')) not like \'%influ%\' and order_name not in (\'2431093\',\'2422140\',\'2425364\',\'2430652\',\'2422237\',\'2420623\',\'2429832\',\'2422378\',\'2428311\',\'2429064\',\'2428204\',\'2421343\',\'2431206\',\'2430491\',\'2426682\',\'2426487\',\'2426458\',\'2423575\',\'2422431\',\'2423612\',\'2426625\',\'2428117\',\'2426894\',\'2425461\',\'2426570\',\'2423455\',\'2430777\',\'2426009\',\'2428245\',\'2427269\',\'2430946\',\'2425821\',\'2429986\',\'2429085\',\'2422047\',\'2430789\',\'2420219\',\'2428341\',\'2430444\',\'2426866\',\'2431230\',\'2425839\',\'2430980\',\'2427048\',\'2430597\',\'2420499\',\'2431050\',\'2420271\',\'2426684\',\'2428747\',\'2423523\',\'2431171\',\'2430830\',\'2425325\',\'2428414\',\'2429054\',\'2423596\') and tags not in (\'FLITS_LOGICERP\') and discount_code != \'CNT_4HHD3430RA9W\' group by 1 ), sku_customer_repeat_rate_predata as ( select a.sku_group, a.size, a.customer_id, b.gross_orders from customers_data a left join customer_purchases b on a.customer_id = b.customer_id ), sku_customer_repeat_rate as ( select sku_group, size, count(distinct customer_id) as repeat_customers from sku_customer_repeat_rate_predata where gross_orders > 1 group by 1,2 ) SELECT distinct inv.sku_group, inv.size, live.live_date, round(COALESCE(inv.inventory, 0),0) AS inventory, ifnull(sales.first_30d_sales, 0) AS first_30d_sales, COALESCE(sales.second_30d_sales, 0) AS second_30d_sales, ifnull(return.first_30d_returns,0) first_30d_returns, COALESCE( sold.sold_out_in, CASE WHEN DATEDIFF(DAY,live.live_date,CURRENT_DATE) >= 90 then ROUND(DIV0(IFNULL(inv.inventory, 0), (IFNULL(sales.first_30d_sales, 0) + IFNULL(sales.second_30d_sales, 0) + IFNULL(sales.last_30d_sales, 0))/ 3) * 30, 0) WHEN DATEDIFF(DAY, live.live_date, CURRENT_DATE) < 90 AND DATEDIFF(DAY, live.live_date, CURRENT_DATE) >= 30 THEN ROUND(DIV0(IFNULL(inv.inventory, 0), (IFNULL(sales.first_30d_sales, 0) + IFNULL(sales.second_30d_sales, 0)) / NULLIF(DATEDIFF(DAY, live.live_date, CURRENT_DATE), 0)), 0) ELSE ROUND(DIV0(IFNULL(inv.inventory, 0), (IFNULL(sales.first_30d_sales, 0))/ DATEDIFF(DAY,live.live_date,CURRENT_DATE)), 0) END ) AS sold_out_in, meta.product_type, meta.price, meta.occassion, meta.print_design, meta.collar, meta.material, meta.sleeve_type, meta.fit, meta.style, meta.color, meta.closure, meta.designs, meta.length, clicks.first_30d_clicks, ifnull(acq.acq_customers,0) as acq_customers, ifnull(repeat.repeat_customers,0) as repeat_customers FROM starting_inventory inv LEFT JOIN final_sales_data sales ON inv.sku_group = sales.sku_group AND inv.size = sales.size LEFT JOIN final_live_date live ON inv.sku_group = live.sku_group LEFT JOIN returns_data return on inv.sku_group = return.sku_group and inv.size = return.size LEFT JOIN metafields meta on inv.sku_group = meta.sku_group LEFT JOIN clicks clicks on inv.sku_group = clicks.sku_group LEFT JOIN acq_customers acq on inv.sku_group = acq.sku_group and inv.size = acq.size LEFT JOIN sku_customer_repeat_rate repeat on inv.sku_group = repeat.sku_group and inv.size = repeat.size LEFT JOIN days_to_reach_target sold on inv.sku_group = sold.sku_group and inv.size = sold.size where round(COALESCE(inv.inventory, 0),0) - COALESCE(sales.first_30d_sales, 0) + ifnull(return.first_30d_returns,0) >= 0 and round(COALESCE(inv.inventory, 0),0) > 10 and COALESCE(sales.first_30d_sales, 0) > 0 ) ; create or replace table snitch_db.maplemonk.design_fact_items as ( select sku_group, live_date, product_type, price, occassion, print_design, collar, material, sleeve_type, fit, style, color, closure, length, designs, sum(inventory) as inventory, sum(first_30d_sales) as first_30d_sales, sum(second_30d_sales) as second_30d_sales, sum(first_30d_returns) as first_30d_returns, round(avg(sold_out_in),0) as sold_out_in, sum(first_30d_clicks) as first_30d_clicks, sum(acq_customers) as acq_customers, sum(repeat_customers) as repeat_customers, count(case when sold_out_in < 70 then sku_group end) as hit_sizes from snitch_db.maplemonk.design_fact_items_size_level group by sku_group, live_date, product_type, price, occassion, print_design, collar, material, sleeve_type, fit, style, color, closure, length, designs )",
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
            