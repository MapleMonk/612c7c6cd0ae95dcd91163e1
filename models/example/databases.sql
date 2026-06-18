{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.Quickshift_Purchase_Orders_Fact_Items; CREATE TABLE public.Quickshift_Purchase_Orders_Fact_Items as select po.*, wm.\"Mapped Warehouse Name\", wm.\"Client Name\" from ( select * from public.Unicommerce_Purchase_Orders_Fact_Items union ALL select * from public.Easyecom_Purchase_Orders_Fact_Items ) po left join (select * from (select *, row_number() over (partition by \"facility name\" order by \"facility name\") rw from public.gs_warehouse_mapping ) where rw = 1) wm on lower(wm.\"facility name\") = lower(po.po_created_warehouse) ; DROP TABLE IF EXISTS public.Quickshift_GRN_Fact_Items; CREATE TABLE public.Quickshift_GRN_Fact_Items as select grn.*, wm.\"Mapped Warehouse Name\", wm.\"Client Name\" from ( select * from public.quickshift_easyecom_grn_fact_items union all select * from public.quickshift_unicommerce_grn_fact_items ) grn left join (select * from (select *, row_number() over (partition by \"facility name\" order by \"facility name\") rw from public.gs_warehouse_mapping ) where rw = 1) wm on lower(wm.\"facility name\") = lower(grn.warehouse) ; DROP TABLE IF EXISTS public.Quickshift_Putaway_Fact_Items; CREATE TABLE public.Quickshift_Putaway_Fact_Items as select * from public.quickshift_easyecom_putaway_report union ALL select * from public.quickshift_unicommerce_putaway_report ; DROP TABLE IF EXISTS public.Quickshift_Returns_Fact_Items; CREATE TABLE public.Quickshift_Returns_Fact_Items as select r.* from ( select * from public.easyecom_all_returns union ALL select * from public.unicommerce_all_returns ) r ; DROP TABLE IF EXISTS public.Quickshift_Purchase_GRN_Put_away_Fact_Items; CREATE TABLE public.Quickshift_Purchase_GRN_Put_away_Fact_Items as select po.po_number ,date(po.po_created_date) po_created_date ,po.sku ,po.original_quantity ,po.pending_quantity ,grn.Received_Quantity ,po.po_status ,po.data_source ,po.\"mapped warehouse name\" ,po.\"client name\" from public.Quickshift_Purchase_Orders_Fact_Items po left join ( select po_number ,date(po_created_date) po_created_date ,warehouse ,sku ,data_source ,\"mapped warehouse name\" ,\"client name\" ,sum(received_quantity) Received_Quantity from public.Quickshift_GRN_Fact_Items group by 1,2,3,4,5,6,7 ) grn on po.po_number = grn.po_number and po.sku = grn.sku and date(po.po_created_date) = date(grn.po_created_date) and po.po_created_warehouse = grn.warehouse and po.data_source = grn.data_source;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            