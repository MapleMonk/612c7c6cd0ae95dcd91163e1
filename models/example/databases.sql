{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.izf_slikk_fact_items as select cast(sp as float64) as selling_price, hsn, cast(mrp as float64) as mrp, sku, cast(tax as float64) as tax, coalesce(p.name,s.name) as product_name, size, brand, color, cast(discount as float64) as discount, cast(tax_rate as float64) as tax_rate, Date(parse_date(\'%d-%m-%Y\',order_date)) as order_date, invoice_id, case when upper(payment_mode) like \'%POD%\' then \'COD\' else \'PREPAID\' end as payment_mode, cast(total_amount as float64) as total_amount, delivery_type, cast(taxable_amount as float64) as gross_sales_before_tax, cast(total_quantity as int64) as quantity, REGEXP_EXTRACT(delivery_address, r\'(\d{6})$\') AS pincode, REGEXP_EXTRACT(delivery_address, r\',([^,]+)-\d{6}$\') AS state, REGEXP_EXTRACT(delivery_address, r\',([^,]+),[^,]+-\d{6}$\') AS city, coalesce(concat(\'<img src=\"\',p.image_url,\'\"width=\"70\">\'),concat(\'<img src=\"\',s.image,\'\"width=\"70\">\')) as Image, p.category as product_category from maplemonk.gs_izf_slikk_sales s left join (select * from (select sku as skucode, Product_Name as name, category_name as category, product_image_url as image_url, row_number() over (partition by SKU order by 1) rw from izf-wh.maplemonk.easyecom_izf_product_master ) where rw = 1 ) p on lower(s.sku) = lower(p.skucode) ;",
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
            