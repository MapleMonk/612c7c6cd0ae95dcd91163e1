{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.channel_sales_inventory as with main_data as ( select sku_group,product_name,category from availability_master_v2 qualify row_number() over (partition by sku_group order by status desc) = 1 ), overall as ( select sku_group, sum(gross_quantity) as overall_quantity, sum(gross_sales) as overall_sales, sum(discount_amount) as overall_discount from horizontal_sales_categories group by 1 ), inv as ( select sku_group, inventory, offline_inventory, STORE_ROS_7, SHOPIFY_ROS_7, MARKETPLACE_ROS_7, STORE_ROS_30, SHOPIFY_ROS_30, MARKETPLACE_ROS_30, last_inward from product_journey ), image as ( select sku_group, preview_image as image from products_db_copy qualify row_number() over (partition by sku_group order by status desc) = 1 ), prod as ( select upper(trim(REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1, LEN(sku))))) as sku_group, order_date, edd as expected_delivery_date, sum(ifnull(total_qty,0)) as quant_orderd, sum(ifnull(received_qty,0)) as quant_recieved from PURCHASE_ORDER_JOURNEY group by 1,2,3 qualify row_number() over (partition by upper(trim(REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1, LEN(sku))))) order by order_date desc) = 1 ), moq as ( select sku_group, final_order_quant as quant_required from nos__final_moq ) select m.*, ifnull(c.inventory,0) as online_inv, ifnull(c.offline_inventory,0) as offline_inv, ifnull(d.overall_quantity,0) as overall_quantity, ifnull(d.overall_sales,0) as overall_sales, ifnull(d.overall_discount,0) as overall_discount, i.image, ifnull(STORE_ROS_7,0) as STORE_ROS_7, ifnull(SHOPIFY_ROS_7,0) as SHOPIFY_ROS_7, ifnull(MARKETPLACE_ROS_7,0) as MARKETPLACE_ROS_7, ifnull(STORE_ROS_30,0) as STORE_ROS_30, ifnull(SHOPIFY_ROS_30,0) as SHOPIFY_ROS_30, ifnull(MARKETPLACE_ROS_30,0) as MARKETPLACE_ROS_30, c.last_inward, p.order_date as order_date, p.expected_delivery_date as expected_delivery_date, p.quant_orderd, p.quant_recieved, moq.quant_required from main_data m left join overall d on m.sku_group = d.sku_group left join inv c on m.sku_group = c.sku_group left join image i on m.sku_group = i.sku_group left join prod p on m.sku_group = p.sku_group left join moq moq on m.sku_group = moq.sku_group ; create or replace table snitch_db.maplemonk.sales_inv_timeseries as with sales as ( select sku_group, date, sum(gross_quantity) as sales from horizontal_sales_categories where date >= \'2024-05-30\' group by 1,2 ), inv as ( select sku_group, date, sum(total_inventory) as inv from cut_size_analysis where date >= \'2024-05-30\' group by 1,2 ) select a.*, b.inv from sales a left join inv b on a.sku_group = b.sku_group and a.date = b.date ;",
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
            