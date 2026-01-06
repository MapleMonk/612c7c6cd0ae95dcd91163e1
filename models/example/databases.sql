{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.pronk_sku_profitability_report as with all_sku_dates as ( SELECT DISTINCT upper(SKU) sku, c.date_day, d.final_marketplace marketplace, d.PRODUCT_NAME_Final, d.Product_Category, d.Product_Sub_Category, d.Product_Colour, d.Product_Size, d.Product_DT_Code, d.Gender FROM pronk-wh.maplemonk.pronk_SALES_CONSOLIDATED d CROSS JOIN ( SELECT date_day FROM UNNEST(GENERATE_DATE_ARRAY(DATE \'2024-01-01\', CURRENT_DATE())) AS date_day ) AS c ) select fi.sku, date_day, marketplace, PRODUCT_NAME_Final, Product_Category, Product_Sub_Category, Product_Colour, Product_Size, Product_DT_Code, Gender, total_sales, total_quantity, total_orders, i.avg_grn_cost as grn_cost, case when (Product_DT_Code is null or Product_DT_Code = \'NA\') then i.avg_grn_cost else (i.avg_grn_cost + 60) end as avg_grn_cost from all_sku_dates FI left join (select upper(sku) sku, date(order_date) date, sum(ifnull(selling_price,0)) as total_sales, sum(ifnull(quantity,0)) as total_quantity, count(distinct order_id) as total_orders from maplemonk.pronk_sales_consolidated where rto_flag = 0 and dto_flag = 0 and (order_status is null or not(order_status like any (\'%cancel%\',\'%return%\',\'%undelivered%\',\'%exchange%\'))) and (oms_order_status is null or not(oms_order_status like any (\'%cancel%\',\'%return%\',\'%undelivered%\',\'%exchange%\'))) and (final_shipping_status is null or not(final_shipping_status like any (\'%cancel%\',\'%return%\',\'%undelivered%\',\'%exchange%\'))) group by 1,2 ) s on s.sku = fi.sku and date(s.date) = date(fi.date_day) left join (select upper(trim(sku)) as sku, avg(cast(grn_cost as float64)) as avg_grn_cost from maplemonk.Easyecom_full_inventory_report group by 1 qualify row_number() over (partition by sku order by 1)=1 )i on lower(i.sku) = lower(fi.sku) ; create or replace table maplemonk.pronk_daily_profitability_report as select date_day, s.marketplace, sum(ifnull(total_orders,0)) as total_orders, sum(ifnull(total_quantity,0)) as total_quantity, sum(ifnull(total_sales,0)) as total_sales, sum(ifnull(grn_cost,0)*ifnull(total_quantity,0)) as grn_cost, sum(ifnull(avg_grn_cost,0)*ifnull(total_quantity,0)) as avg_grn_cost, avg(ifnull(spend,0)) as adv_spends from maplemonk.pronk_sku_profitability_report s left join (select date, case when lower(channel) like any (\'%facebook%\', \'%google%\') then \'WEBSITE\' when lower(channel) like any (\'%amazon%\') then \'AMAZON\' when lower(channel) like \'%flipkart%\' then \'FLIPKART\' when lower(channel) like \'%myntra%\' then \'MYNTRA\' else upper(channel) end as Marketplace, sum(ifnull(spend,0)) as spend from pronk-wh.maplemonk.pronk_MARKETING_CONSOLIDATED group by 1,2) m on m.date = s.date_day and lower(m.marketplace) = lower(s.marketplace) group by 1,2;",
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
            