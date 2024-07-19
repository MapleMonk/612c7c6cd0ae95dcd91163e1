{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table hox_db.maplemonk.hox_db_blanko_summary_stock_reorder_FBA_VSKU as with sku_masters as ( select * from (select distinct replace(marketplace_sku,\'\`\',\'\') marketplace_sku ,replace(master_sku,\'\`\',\'\') master_sku ,row_number() over (partition by replace(marketplace_sku,\'\`\',\'\') order by 1) rw from HOX_DB.MAPLEMONK.sku_master ) where rw=1 ), sales_quantity as ( select case when lower(source) = \'amazon\' then \'Amazon FBA\' else \'Easyecom\' end as source, reference_code, date(order_date) as order_date, coalesce(b.master_sku, a.sku_code)as sku_code, case when lower(source) = \'amazon\' then \'Amazon warehouse\' else WAREHOUSE end as WAREHOUSE, sum(quantity) as total_quantity from HOX_DB.MapleMonk.HOX_DB_SALES_CONSOLIDATED a left join sku_masters b on lower(a.sku_code) = lower(b.marketplace_sku) where lower(order_status) not like \'%cancel%\' and sku_code not in (\'\',\'BLKCAP_FR_02\', \'BLKFREECAP_01\', \'BL_FREE_TSHIRT_01\',\'BLKCAP_FR_01\', \'450\') and lower(source) = \'amazon\' group by 1,2,3,4,5 ), fba_stock as ( select coalesce(a.master_sku, o.SKU) as SKU, \'Amazon warehouse\' as Warehouse, sum(fba_inventory) as total_quantity_FBA from ( select o.sku, (SUM(\"afn-reserved-quantity\") + sum(\"afn-fulfillable-quantity\")) AS fba_inventory from ( Select * , ROW_NUMBER() OVER(PARTITION BY SKU order by _AIRBYTE_EMITTED_AT desc) as rn from HOX_DB.MapleMonk.GET_FBA_MYI_ALL_INVENTORY_DATA where lower(\"afn-listing-exists\") = \'yes\' )as o where rn = 1 group by 1 )o left join ( select distinct marketplace_sku,master_sku from sku_masters )a on lower(o.SKU) = lower(a.marketplace_sku) group by 1,2 ), base_final as ( select sku_code as new_sku, round(Sum(case when source = \'Amazon FBA\' and (order_date between (current_date - 7) and (current_date - 1)) then total_quantity end)/7,2) as FBA_DRR_7day, round(Sum(case when source = \'Amazon FBA\' and (order_date between (current_date - 30) and (current_date - 1)) then total_quantity end)/30,2) as FBA_DRR_30day, round(Sum(case when source = \'Amazon FBA\' and (order_date between (current_date - 60) and (current_date - 1)) then total_quantity end)/60,2) as FBA_DRR_60day from sales_quantity Group by 1 ) select o.*, case when \"Days of Inventory FBA 30day\" <= 60 then \'YES\' else \'NO\' end as \"Re-order required FBA\", case when \"Days of Inventory FBA 30day\" <= 60 then round(FBA_DRR_30day * 75,0) else null end as \"Re-order units FBA\" from ( Select o.*, round(coalesce((total_quantity_FBA *1.00 / nullif(FBA_DRR_30day,0)),0),2) as \"Days of Inventory FBA 30day\", round(coalesce((total_quantity_FBA *1.00 / nullif(FBA_DRR_60day,0)),0),2) as \"Days of Inventory FBA 60day\" from ( select a.* , b.total_quantity_FBA from base_final a LEFT JOIN fba_stock b ON lower(a.new_sku) = lower(b.sku) )as o )as o where new_sku <> \'BLKCAP_FR_01\' order by 1",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from HOX_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            