{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.contribution_margin as select order_date, daily_spend, marketplace_mapped, total_discount, unit_price_with_tax, unit_price_without_tax, gross_without_ajio+ajio_sales as total_gross_sales, coalesce(cancelled_sales_ajio,0)+coalesce(cancelled_sales_without_ajio,0) as cancelled_sales, coalesce(return_sales_ajio,0)+coalesce(return_sales_without_ajio,0) as return_sales, coalesce(cancelled_cost_without_tax,0) as cancelled_cost_without_tax, coalesce(return_cost_without_tax,0) as return_cost_without_tax, total_taxes, CASE WHEN marketplace_mapped in (\'AJIO\', \'MYNTRA\', \'FLIPKART\', \'AMAZON\') THEN (total_gross_sales-cancelled_sales-return_sales)*0.3 ELSE 0 END AS mp_commissions from ( select order_date,daily_spend,marketplace_mapped, sum(mrp_unicommerce.MRP-(selling_price-tax)) as total_discount, sum(tax) as total_taxes, sum(unit_price_with_tax) as unit_price_with_tax, sum(unit_price_without_tax) as unit_price_without_tax, sum(CASE WHEN marketplace_mapped not in (\'AJIO\') THEN selling_price-tax ELSE 0 END) as gross_without_ajio, sum(CASE WHEN ORDER_STATUS in (\'CANCELLED\') and marketplace_mapped in (\'AJIO\') THEN (selling_price-tax)*1.42 ELSE 0 END) as cancelled_sales_ajio, sum(CASE WHEN ORDER_STATUS in (\'CANCELLED\') and marketplace_mapped not in (\'AJIO\') THEN (selling_price-tax) ELSE 0 END) as cancelled_sales_without_ajio, sum(CASE WHEN marketplace_mapped in (\'AJIO\') THEN (selling_price-tax)*1.42 ELSE 0 END) as ajio_sales, SUM(CASE WHEN return_flag = 1 and marketplace_mapped in (\'AJIO\') THEN (selling_price-tax)*1.42 END) AS return_sales_ajio, SUM(CASE WHEN return_flag = 1 and marketplace_mapped not in (\'AJIO\') THEN (selling_price-tax) END) AS return_sales_without_ajio, sum(CASE WHEN ORDER_STATUS in (\'CANCELLED\') THEN unit_price_without_tax ELSE 0 END) as cancelled_cost_without_tax, SUM(CASE WHEN return_flag = 1 THEN unit_price_without_tax END) AS return_cost_without_tax from snitch_db.maplemonk.unicommerce_fact_items_snitch LEFT JOIN (select distinct \"Vendor skuCode\" as sku, max(\"Unit price with tax\") as unit_price_with_tax,max(\"Unit price without tax\") as unit_price_without_tax from snitch_db.maplemonk.unicommerce_itembarcode_report group by sku) barcode ON unicommerce_fact_items_snitch.sku=barcode.sku LEFT JOIN (select distinct sku, price as MRP from snitch_db.snitch.product_dim group by sku,MRP) mrp_unicommerce ON unicommerce_fact_items_snitch.sku=mrp_unicommerce.sku LEFT JOIN (select date as spend_date,sum(spend) as daily_spend from snitch_db.maplemonk.marketing_consolidated_snitch group by spend_date order by spend_date desc) spends ON unicommerce_fact_items_snitch.order_date=spends.spend_date WHERE year(order_date)=2023 and month(order_date)>=4 group by order_date, marketplace_mapped, daily_spend order by order_date desc, marketplace_mapped )",
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
                        