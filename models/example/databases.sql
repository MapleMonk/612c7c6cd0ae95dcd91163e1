{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_doq_recovery_fact_items as select grn_created_date as date, replace(replace(replace(g.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\') sku, sum(ifnull(received_quantity,0)) grn_received_quantity, avg(ifnull(d.doq,0)) doq, sum(ifnull(availableinventory,0)) opening_inventory from maplemonk.saadaa_grn_fact_items g left join ( select date_day as date, sku, avg(greatest(doq_365, weighted_doq_45)) as doq from maplemonk.saadaa_inventory_planning where warehouse = \'Main Warehouse\' group by 1,2 ) d on lower(replace(replace(replace(g.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\')) = lower(d.sku) and d.date = g.grn_created_date left join (with inventory as (SELECT date(DATETIME_SUB(PARSE_DATETIME(\'%Y-%m-%d %H:%M:%S\', Report_Generated_Date),INTERVAL -5 HOUR)) DATA_FETCH_DATE, replace(replace(replace(replace(sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\'),\'`\',\'\') sku, cast(Available_Quantity as INT64) Available_Quantity, from maplemonk.easyecom_saada_inventory_snapshot where location = \'SAADAA SUSTAINABLE DESIGNS AND TECHNOLOGIES PRIVATE LIMITED\' qualify row_number() over (partition by location, Company_Token, SKU, cast(DATA_FETCH_DATE as date) order by DATA_FETCH_DATE) = 1 ) SELECT cast(DATA_FETCH_DATE as date) DATA_FETCH_DATE, SKU, SUM(IFNULL(Available_Quantity,0)) AS AVAILABLEINVENTORY FROM Inventory GROUP BY 1, 2 ) i on lower(replace(replace(replace(g.sku,\'-\',\'_\'),\'_\',\'\'),\' \',\'\')) = lower(i.sku) and i.DATA_FETCH_DATE = g.grn_created_date group by 1,2 ;",
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
            