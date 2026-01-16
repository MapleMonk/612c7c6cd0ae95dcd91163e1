{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_inward_prioritisation as with monthly_quantity as ( select upper(commonsku) as SKU, upper(product_name_final) as Product_Name, date_trunc(order_date,month) as month, upper(marketplace) as marketplace, sum(quantity) as monthly_units_sold from maplemonk.saadaa_sales_consolidated group by 1,2,3,4 ) select upper(s.commonsku) as SKU, upper(product_name_final) as Product_Name, upper(product_category) as Product_category, date(s.order_date) as date, upper(s.marketplace) as marketplace, max(order_date) as last_date_of_sale, avg(p.mrp) mrp, avg(m.monthly_units_sold) as monthly_units_sold, round(safe_divide(sum(ifnull(selling_price,0)),sum(quantity)),2) as asp, sum(quantity) as units_sold, sum(ifnull(selling_price,0)) as selling_price from maplemonk.saadaa_sales_consolidated s left join (select * from (select commonsku, product_name name, mrp, row_number() over (partition by replace(commonsku,\' \',\'\') order by 1) rw from saadaa-wh.maplemonk.saadaa_final_sku_master) where rw = 1 ) p on lower(replace(s.commonsku,\' \',\'\')) = lower(replace(p.commonsku,\' \',\'\')) left join monthly_quantity m on m.sku = upper(s.commonsku) and m.month = date_trunc(s.order_date,month) and m.marketplace = upper(s.marketplace) and m.product_name = upper(s.product_name_final) where s.gross_order_type = \'Gross Sale Orders\' and s.commonsku is not null group by 1,2,3,4,5 ;",
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
            