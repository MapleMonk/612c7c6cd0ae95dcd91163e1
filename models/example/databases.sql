{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_inward_prioritisation as with monthly_quantity as ( select upper(commonsku) as SKU, upper(product_name_final) as Product_Name, date_trunc(order_date,month) as month, upper(marketplace) as marketplace, sum(quantity) as monthly_units_sold from maplemonk.saadaa_sales_consolidated group by 1,2,3,4 ), purchase_orders as ( select sku, date(po_created_date) as po_created_date, pending_quantity as po_wise_pending_quantity, po_detail_id as open_po_number, vendor_name, expected_delivery_date, row_number() over (partition by sku order by expected_delivery_date asc,po_created_date asc) as rw from maplemonk.saadaa_purchase_order_fact_items where pending_quantity > 0 and lower(po_status) like \'%approved%\' ) select upper(s.commonsku) as SKU, upper(product_name_final) as Product_Name, upper(product_category) as Product_category, date(s.order_date) as date, upper(s.marketplace) as marketplace, max(po_created_date) po_created_date, max(po_wise_pending_quantity) po_wise_pending_quantity, max(open_po_number) as open_po_number, max(vendor_name) vendor_name, max(expected_delivery_date) as expected_delivery_date, max(order_date) as last_date_of_sale, avg(p.mrp) mrp, avg(m.monthly_units_sold) as monthly_units_sold, round(safe_divide(sum(ifnull(selling_price,0)),sum(quantity)),2) as asp, sum(quantity) as units_sold, sum(ifnull(selling_price,0)) as selling_price from maplemonk.saadaa_sales_consolidated s left join (select * from (select commonsku, product_name name, mrp, row_number() over (partition by replace(commonsku,\' \',\'\') order by 1) rw from saadaa-wh.maplemonk.saadaa_final_sku_master) where rw = 1 ) p on lower(replace(s.commonsku,\' \',\'\')) = lower(replace(p.commonsku,\' \',\'\')) left join monthly_quantity m on m.sku = upper(s.commonsku) and m.month = date_trunc(s.order_date,month) and m.marketplace = upper(s.marketplace) and m.product_name = upper(s.product_name_final) left join (select * from purchase_orders where rw=1) po on lower(replace(s.commonsku,\' \',\'\')) = lower(replace(po.sku,\' \',\'\')) where s.gross_order_type = \'Gross Sale Orders\' and s.commonsku is not null group by 1,2,3,4,5 ;",
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
            