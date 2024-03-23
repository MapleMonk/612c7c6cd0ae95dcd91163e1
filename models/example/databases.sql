{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table hox_db.maplemonk.hox_db_blanko_summary_stock_reorder as with sales_quantity as ( select case when lower(source) = \'amazon\' then \'Amazon FBA\' else \'Easyecom\' end as source, reference_code, date(order_date) as order_date, sku_code, sum(quantity) as total_quantity from HOX_DB.MapleMonk.HOX_DB_SALES_CONSOLIDATED where lower(order_status) not like \'%cancel%\' and sku_code not in (\'\',\'BLKCAP_FR_02\', \'BLKFREECAP_01\', \'BL_FREE_TSHIRT_01\',\'BLKCAP_FR_01\', \'450\') group by 1,2,3,4 ), sku_masters as ( select * from (select distinct replace(marketplace_sku,\'\`\',\'\') marketplace_sku ,replace(master_sku,\'\`\',\'\') master_sku ,row_number() over (partition by replace(marketplace_sku,\'\`\',\'\') order by 1) rw from HOX_DB.MAPLEMONK.sku_master ) where rw=1 ), fba_stock as ( select coalesce(c.new_sku , a.sku) as component_sku_name, Sum(ifnull(c.item_count,1) * a.fba_inventory) as QUANTITY from ( select coalesce(a.master_sku, o.SKU) as SKU, fba_inventory from ( select o.sku, (SUM(\"afn-reserved-quantity\") + sum(\"afn-fulfillable-quantity\")) AS fba_inventory from ( Select * , ROW_NUMBER() OVER(PARTITION BY SKU order by _AIRBYTE_EMITTED_AT desc) as rn from HOX_DB.MapleMonk.GET_FBA_MYI_ALL_INVENTORY_DATA )as o where rn = 1 group by 1 )o left join ( select distinct marketplace_sku,master_sku from sku_masters )a on lower(o.SKU) = lower(a.marketplace_sku) )as a left join hox_db.maplemonk.hox_db_Sku_child_mapping c on lower(a.SKU) = lower(c.sku_code) group by 1 order by 2 ), base_final as ( select new_sku, round(Sum(case when source = \'Easyecom\' and (order_date between (current_date - 7) and (current_date - 1)) then final_quantity end)/7, 2) as EE_DRR_7day, round(Sum(case when source = \'Easyecom\' and (order_date between (current_date - 30) and (current_date - 1)) then final_quantity end)/30, 2) as EE_DRR_30day, round(Sum(case when source = \'Easyecom\' and (order_date between (current_date - 60) and (current_date - 1)) then final_quantity end)/60, 2) as EE_DRR_60day, round(Sum(case when source = \'Amazon FBA\' and (order_date between (current_date - 7) and (current_date - 1)) then final_quantity end)/7,2) as FBA_DRR_7day, round(Sum(case when source = \'Amazon FBA\' and (order_date between (current_date - 30) and (current_date - 1)) then final_quantity end)/30,2) as FBA_DRR_30day, round(Sum(case when source = \'Amazon FBA\' and (order_date between (current_date - 60) and (current_date - 1)) then final_quantity end)/60,2) as FBA_DRR_60day from ( select distinct a.* , b.new_sku, round(b.item_count:: int,0) as item_count, (total_quantity * round(b.item_count:: int,0))as final_quantity from sales_quantity a left join hox_db.maplemonk.hox_db_Sku_child_mapping b on lower(a.sku_code) = lower(b.sku_code) )as o Group by 1 ), base_aggregate as ( select distinct o.* , a.QUANTITY as total_quantity_FBA from ( select sku, (sum(AVAILABLE_INVENTORY)) as total_quantity_Emiza from ( select ROW_NUMBER() over (partition by sku, new_location order by data_fetch_date:: timestamp desc) rw, * from ( select case when trim(UPPER(location)) in (\'BLANKO GGN\', \'MOJOJOJO CREATORS PVT LTD GGN\') then \'BLANKO GGN\' when trim(upper(location)) in (\'BLANKO BLR\', \'MOJOJOJO CREATORS PVT LTD BLR\') then \'BLANKO BLR\' else location end as new_location, * from hox_db.maplemonk.hox_db_blanko_summary_inventory_sku_sales )o )as o where o.rw = 1 group by 1 order by 2 desc )o left join fba_stock a on lower(o.sku) = lower(a.component_sku_name) ) select o.*, case when \"Run Rate Emiza 30day\" <= 60 then \'YES\' else \'NO\' end as \"Re-order required Emiza\", case when \"Run Rate Emiza 30day\" <= 60 then round(EE_DRR_30day * 75,0) else null end as \"Re-order units\", case when \"Run Rate FBA 30day\" <= 60 then \'YES\' else \'NO\' end as \"Re-order required FBA\", case when \"Run Rate FBA 30day\" <= 60 then round(FBA_DRR_30day * 75,0) else null end as \"Re-order units FBA\", from ( Select o.*, round(coalesce((total_quantity_Emiza *1.00 / nullif(EE_DRR_30day,0)),0),2) as \"Run Rate Emiza 30day\", round(coalesce((total_quantity_Emiza *1.00 / nullif(EE_DRR_60day,0)),0),2) as \"Run Rate Emiza 60day\", round(coalesce((total_quantity_FBA *1.00 / nullif(FBA_DRR_30day,0)),0),2) as \"Run Rate FBA 30day\", round(coalesce((total_quantity_FBA *1.00 / nullif(FBA_DRR_60day,0)),0),2) as \"Run Rate FBA 60day\" from ( select a.* , b.total_quantity_Emiza, b.total_quantity_FBA from base_final a LEFT JOIN base_aggregate b ON lower(a.new_sku) = lower(b.sku) )as o )as o where new_sku <> \'BLKCAP_FR_01\' order by 1",
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
                        