{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table lilgoodness_db.maplemonk.secondary_sales_cost_lg as with marketing_spend as ( select date ,\'SHOPIFY_INDIA\' Marketplace ,sum(ifnull(spend,0)) Spend ,sum(ifnull(impressions,0)) IMPRESSIONS ,sum(ifnull(clicks,0)) Clicks ,sum(ifnull(conversions,0)) Orders_as_per_marketing_platform ,sum(ifnull(conversion_value,0)) Sales_as_per_marketing_platform from marketing_consolidated_lg group by 1,2 union all select DATE ,\'AMAZON\' Marketplace ,sum(ifnull(spend,0)) Spend ,sum(ifnull(impressions,0)) IMPRESSIONS ,sum(ifnull(clicks,0)) Clicks ,sum(ifnull(conversions,0)) Orders_as_per_marketing_platform ,sum(ifnull(sales,0)) Sales_as_per_marketing_platform from lilgoodness_db.maplemonk.AMAZONADS_IN_MARKETING_LG group by 1,2 union all select try_to_date(\"Date(MM_DD_YYYY)\", \'MM/DD/YYYY\') date ,upper(MARKETPLACE) ,sum(ifnull(spend,0)) Spend ,sum(ifnull(impressions,0)) IMPRESSIONS ,sum(ifnull(clicks,0)) Clicks ,sum(ifnull(conversion,0)) Orders_as_per_marketing_platform ,sum(ifnull(sales_from_marketing,0)) Sales_as_per_marketing_platform from lilgoodness_db.maplemonk.marketplace_marketing_spend group by 1, 2 order by 1 desc ) ,orders as ( select order_date::date Date ,upper(shop_name) MARKETPLACE ,sum(ifnull(mrp_sales,0)) MRP_SALES ,sum(ifnull(mapped_total_mrp_sales,0)) MAPPED_MRP_SALES ,sum(ifnull(selling_price,0)) Total_Sales ,sum(ifnull(suborder_quantity,0)) Total_Units ,count(distinct order_id) Orders ,sum(ifnull(discount_mrp,0)) MRP_DISCOUNT ,sum(ifnull(MAPPED_TOTAL_DISCOUNT,0)) MAPPED_MRP_DISCOUNT from lilgoodness_db.maplemonk.secondary_sales_consolidated_lg group by 1,2 ) select coalesce(O.Date, M.Date) Date ,coalesce(O.Marketplace, M.Marketplace) Marketplace ,O.MRP_SALES ,O.MAPPED_MRP_SALES ,O.Total_Sales ,O.MRP_DISCOUNT ,O.MAPPED_MRP_DISCOUNT ,O.Total_units ,O.orders ,M.Spend ,M.IMPRESSIONS ,M.Clicks ,M.Orders_as_per_marketing_platform ,M.Sales_as_per_marketing_platform from orders O full outer join marketing_spend M on O.date=M.date and lower(O.Marketplace) = lower(M.Marketplace) ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        