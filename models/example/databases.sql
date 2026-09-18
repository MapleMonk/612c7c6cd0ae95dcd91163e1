{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `project-dea872a4-8550-4cff-a8c`.maplemonk.miniklub_targets_fact_items AS WITH base AS ( SELECT *, CASE When lower(marketplace) like \'%miniklub%\' then \'Miniklub\' Else \'Marketplaces\' End as marketplace_grouping FROM `project-dea872a4-8550-4cff-a8c`.maplemonk.project_dea872a4_8550_4cff_a8c_sales_consolidated ) SELECT order_date, marketplace_grouping, \'1.Actual\' AS sale_type, cast(Quantity as FLOAT64) Quantity_, Discount, Selling_Price, cancelled_sales, unfulfillable_sales, cancelled_quantity, unfulfillable_quantity, cast(NULL as FLOAT64) as target_qty_per_order_item, cast(NULL as FLOAT64) as target_markdown, cast(NULL as FLOAT64) as target_net_sales_per_order_item, cast(NULL as FLOAT64) as target_asp, cast(NULL as FLOAT64) as target_drr, days_in_month From base UNION ALL SELECT order_date, marketplace_grouping, \'2.Target\' AS sale_type, NULL as quantity_, NULL as discount, NULL as selling_price, NULL as cancelled_sales, NULL as unfulfillable_sales, NULL as cancelled_quantity, NULL as unfulfillable_quantity, target_qty_per_order_item, target_markdown, target_net_sales_per_order_item, target_asp, target_drr, days_in_month From base UNION ALL SELECT order_date, marketplace_grouping, \'3.Achievement %\' AS sale_type, cast(Quantity as FLOAT64) Quantity_, Discount, Selling_Price, cancelled_sales, unfulfillable_sales, cancelled_quantity, unfulfillable_quantity, target_qty_per_order_item, target_markdown, target_net_sales_per_order_item, target_asp, target_drr, days_in_month From base ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            