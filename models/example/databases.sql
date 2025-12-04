{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_oos_tracker as with all_sku_dates AS ( SELECT DISTINCT c.date_day, upper(replace(replace(replace(replace(replace(d.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\'),\'`\',\'\'),\'\\'\',\'\')) sku, d.location, FROM maplemonk.easyecom_saada_inventory_snapshot AS d CROSS JOIN ( SELECT date_day FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(CURRENT_DATE(), YEAR), CURRENT_DATE())) AS date_day ) AS c ), inventory as ( SELECT d.date_day, replace(replace(replace(replace(replace(d.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\'),\'`\',\'\'),\'\\'\',\'\') sku, case when d.location like \'%FBA%\' then \'FBA Warehouse\' when d.location = \'SAADAA SUSTAINABLE DESIGNS AND TECHNOLOGIES PRIVATE LIMITED\' then \'Main Warehouse\' when d.location = \'Holisol - BLR\' then \'Holisol - BLR\' when upper(d.location) = \'HOLISOL-MH\' then \'Holisol - MH\' end as Warehouse, CASE WHEN ifnull(sum(cast(Available_Quantity as INT64)),0) < 4 THEN 1 ELSE 0 END AS is_oos, sum(cast(Available_Quantity as INT64)) as Available_Quantity FROM all_sku_dates d LEFT JOIN maplemonk.easyecom_saada_inventory_snapshot i on lower(replace(replace(replace(replace(replace(i.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\'),\'`\',\'\'),\'\\'\',\'\')) = lower(replace(replace(replace(replace(replace(d.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\'),\'`\',\'\'),\'\\'\',\'\')) and cast(d.date_day as date) = cast(left(i.report_generated_date,10) as date) and i.location = d.location GROUP BY 1,2,3 ) SELECT i.*, p.category, p.size, ip.weighted_doq_45 doq, o.OOS_in_last_45_days FROM inventory i LEFT JOIN (select * from (select replace(marketplace_sku,\' \',\'\') skucode, upper(replace(replace(replace(commonsku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\')) commonsku, cast(category as string) category, Size, row_number() over (partition by replace(replace(replace(commonsku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\') order by 1) rw from saadaa-wh.maplemonk.saadaa_final_sku_master ) where rw = 1 ) p ON lower(i.sku) = lower(p.commonsku) LEFT JOIN maplemonk.saadaa_inventory_planning ip ON ip.date_day = cast(i.date_day as date) and lower(ip.sku) = lower(i.sku) and ip.warehouse = i.warehouse left join ( select sku, warehouse, sum(is_oos) as OOS_in_last_45_days from inventory where DATE_DIFF(current_date(), date_day, DAY) < 45 group by 1,2 ) o on o.sku = i.sku and o.warehouse = i.warehouse;",
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
            