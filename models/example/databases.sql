{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.izf_monthly_product_profitability_fact_items as with unique_skus as ( select distinct sku_style, sku, marketplace, sku_size from maplemonk.izf_sales_consolidated ), all_sku_dates as ( select c.date_day, u.* from unique_skus u cross join ( select date_day from unnest(generate_date_array(date \'2025-04-01\', current_date())) as date_day ) as c ), sales_agg as ( select sku, marketplace, date(order_date) as order_date, sum(ifnull(selling_price, 0)) as total_sales, count(distinct order_id) as total_orders, sum(ifnull(quantity, 0)) as total_quantity from maplemonk.izf_sales_consolidated where not(order_status in (\'CANCELLED\',\'RETURNED\') and lower(marketplace) like \'%b2b%\') group by 1, 2, 3 ), returns_agg as ( select UPPER(sku) as sku, marketplace, date(order_date) as order_date, sum(ifnull(returned_quantity_btw_7_37_days, 0)) as customer_returns_7_37, sum(ifnull(rto_quantity_btw_7_37_days, 0)) as rto_returns_7_37, sum(ifnull(sold_quantity_btw_7_37_days, 0)) as sold_quantity_7_37 from izf-wh.maplemonk.izf_returns_consolidated group by 1,2,3 order by 1,2,3 ), product_master as ( select upper(split(lower(sku), \'-\')[offset(0)]) as style_lookup, max(product_image_url) as style_image from izf-wh.maplemonk.easyecom_izf_product_master group by 1 ), cogs as (select sku_code, cogs from `MapleMonk.GS_SKU_MRP_COGS`) select a.sku_style as style, a.sku_size as size, a.sku, concat(\'<img src=\"\', pm.style_image, \'\" width=\"70\">\') as Style_Image, a.marketplace, c.cogs as COGS, a.date_day as order_date, ifnull(s.total_sales, 0) as total_sales, ifnull(s.total_orders, 0) as total_orders, ifnull(s.total_quantity, 0) as total_quantity, ifnull(r.customer_returns_7_37, 0) as customer_returns_7_37, ifnull(r.rto_returns_7_37, 0) as rto_returns_7_37, ifnull(r.sold_quantity_7_37, 0) as sold_quantity_7_37 from all_sku_dates a left join cogs c on a.sku = c.sku_code left join sales_agg s on a.sku = s.sku and a.marketplace = s.marketplace and a.date_day = s.order_date left join returns_agg r on a.sku = r.sku and a.marketplace = r.marketplace and a.date_day = r.order_date left join product_master pm on upper(a.sku_style) = pm.style_lookup;",
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
            