{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace Table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_SWIGGY_FACT_ITEMS as select ordered_date::date as Order_Date, item_code, City, units_sold, ifnull(base_mrp,0) * ifnull(units_sold,0) as MRP, ifnull(GMV,0) as GMV, ifnull(invoice_value,0)*ifnull(units_sold,0) as Selling_price_Final, qc.\"Master SKU Code\" as commonsku, s.name as product_name_final, s.category, s.sub_category, area_name, CONCAT( item_code, city, ordered_date,store_id,commonsku) AS order_item from sleepycat_db.maplemonk.sleepycat_db_sales z LEFT JOIN ( SELECT * FROM sleepycat_db.maplemonk.gs_mapping_qc_sku_mapping WHERE lower(platform) = \'swiggy\' QUALIFY ROW_NUMBER() OVER (PARTITION BY priority ORDER BY 1) = 1 ) qc ON upper(qc.priority) = (z.item_code) LEFT JOIN ( select * from sleepycat_db.maplemonk.final_sku_master qualify row_number() over(partition by lower(trim(skucode)) order by 1) = 1 )s on lower(trim(qc.\"Master SKU Code\")) = lower(trim(s.skucode)) left join ( select sku, date, replace(\"Invoice value\",\',\',\'\')::float invoice_value, row_number() over (partition by sku, date order by 1) rw from sleepycat_db.maplemonk.gs_qc_taxable where lower(portal) = \'swiggy\' qualify row_number() over (partition by lower(sku), date order by 1) = 1 ) tv on lower(qc.\"Master SKU Code\") = lower(tv.sku) and date_trunc(month,z.ordered_date::date) = tv.date ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SLEEPYCAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            