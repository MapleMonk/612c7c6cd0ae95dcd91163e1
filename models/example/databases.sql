{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_executive_sales_summary; create table public.anveshan_executive_sales_summary as with sales as ( select marketplace, child_category, date(order_date) as date, sum(coalesce(child_mrp::float8,0)) as mrp_sales, sum(coalesce(selling_price::float8,0)) as sp_sales, sum(coalesce(quantity,0)) as units from public.anveshan_sales_consolidated s group by 1,2,3 ), primary_sales_data as ( select channel, Product_Category as category, purchaseorderdate::date as purchase_order_date, SUM(COALESCE(net_cost::FLOAT8, 0)) AS net_po_amount from public.anveshan_amazon_vendor_purchase_orders group by 1,2,3 union all select channel, category, date(po_date_timestamp) as po_created_date, sum(coalesce(po_total_amount::float8,0)) as net_po_amount from public.anveshan_zepto_po_fact_items group by 1,2,3 union all select channel, category, date(order_date) as po_created_date, sum(coalesce(total_amount::float8,0)) as net_po_amount from PUBLIC.anveshan_blinkit_purchase_orders_fact_items group by 1,2,3 union all select channel, category, date(po_created_date) as po_created_date, sum(coalesce(po_line_value_with_tax::float8,0)) as net_po_amount from public.anveshan_swiggy_po_fact_items group by 1,2,3 ), marketing_data AS ( SELECT date, CASE WHEN (LOWER(channel) LIKE \'%facebook%\' OR LOWER(channel) LIKE \'%google%\') THEN \'WEBSITE\' WHEN LOWER(channel) LIKE \'%amazon%\' THEN \'AMAZON\' WHEN LOWER(channel) LIKE \'%flipkart%\' THEN \'FLIPKART\' WHEN LOWER(channel) LIKE \'%myntra%\' THEN \'MYNTRA\' ELSE UPPER(channel) END AS Marketplace, category, SUM(COALESCE(spend, 0)) AS spend, SUM(COALESCE(impressions, 0)) AS ad_impressions, SUM(COALESCE(clicks, 0)) AS ad_clicks, SUM(COALESCE(conversions, 0)) AS ad_conversions, SUM(COALESCE(conversion_value, 0)) AS ad_sales FROM public.anweshan_MARKETING_CONSOLIDATED GROUP BY 1, 2, 3 ) select s.marketplace, s.child_category, s.date, p.net_po_amount, mrp_sales, sp_sales, units, m.ad_impressions, m.ad_conversions as ad_units, m.ad_conversions, m.ad_sales, m.spend as ad_spend from sales s;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            