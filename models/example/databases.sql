{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table `project-dea872a4-8550-4cff-a8c`.maplemonk.miniklub_targets_fact_items as with base as ( select *, case when lower(marketplace) like \'%miniklub%\' then \'Miniklub\' else \'Marketplaces\' end as marketplace_grouping, safe_cast(sc.selling_price as float64) - ifnull(safe_cast(cg.cogs as float64), 0) - ifnull((ifnull(safe_cast(rc.margin as float64), 0) * safe_cast(sc.selling_price as float64) / 100),0) as trade_margin from `project-dea872a4-8550-4cff-a8c`.maplemonk.project_dea872a4_8550_4cff_a8c_sales_consolidated sc left join maplemonk.miniklub_google_sheet_cogs cg on trim(cg.ean_code) = trim(sc.ean) left join maplemonk.miniklub_google_sheet_retailer_commission rc on trim(upper(rc.channel)) = trim(upper(sc.channel)) where upper(order_status) not in (\'CANCELLED\', \'UNFULFILLABLE\') ) select order_date, marketplace_grouping, \'1.Target\' as sale_type, null as quantity_, null as discount, null as selling_price, null as cancelled_sales, null as unfulfillable_sales, null as cancelled_quantity, null as unfulfillable_quantity, target_qty_per_order_item, target_markdown, target_net_sales_per_order_item, target_asp, target_drr, days_in_month, cast(null as float64) as trade_margin, target_trade__ as target_trade_margin from base union all select order_date, marketplace_grouping, \'2.Actual\' as sale_type, cast(quantity as float64) quantity_, discount, selling_price, cancelled_sales, unfulfillable_sales, cancelled_quantity, unfulfillable_quantity, cast(null as float64) as target_qty_per_order_item, cast(null as float64) as target_markdown, cast(null as float64) as target_net_sales_per_order_item, cast(null as float64) as target_asp, cast(null as float64) as target_drr, days_in_month, trade_margin, cast(null as float64) as target_trade_margin from base union all select order_date, marketplace_grouping, \'3.Achievement %\' as sale_type, cast(quantity as float64) quantity_, discount, selling_price, cancelled_sales, unfulfillable_sales, cancelled_quantity, unfulfillable_quantity, target_qty_per_order_item, target_markdown, target_net_sales_per_order_item, target_asp, target_drr, days_in_month, trade_margin, target_trade__ as target_trade_margin from base;",
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
            