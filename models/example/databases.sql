{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_swiggy_po_fact_items; create table public.anveshan_swiggy_po_fact_items as select \'SWIGGY\' as channel, trim(mrp)::DOUBLE PRECISION as unit_MRP, trim(tax)::DOUBLE PRECISION as tax, trim(city)::varchar as CITY, trim(entity)::varchar as Entity, trim(status)::varchar as PO_status, trim(skucode)::varchar as product_id, p.commonsku, trim(poageing)::bigint as PO_Ageing, trim(poamount)::DOUBLE PRECISION as total_po_amount, trim(ponumber)::varchar as PO_Number, coalesce(p.category,trim(categoryId)::varchar) as category, trim(facilityid)::varchar as Facility_Code, trim(orderedqty)::bigint as PO_Quantity, trim(balancedqty)::bigint as PO_Balance_Quantity, TO_TIMESTAMP(pocreatedat, \'DD/MM/YY HH24:MI\') as po_created_date, trim(receivedqty)::bigint as PO_Received_Quantity, upper(trim(FacilityName)) as Facility_Name, TO_DATE(poexpirydate, \'DD/MM/YY\') as po_expiry_date, TO_TIMESTAMP(pomodifiedat, \'DD/MM/YY HH24:MI\') as po_modified_date, trim(suppliercode) as supplier_code, trim(unitbasedcost)::DOUBLE PRECISION as unit_based_cost, trim(polinevaluewithtax)::DOUBLE PRECISION as po_line_value_with_tax, trim(polinevaluewithouttax)::DOUBLE PRECISION as po_line_value_without_tax, TO_DATE(expecteddeliverydate, \'DD/MM/YY\') as po_expectd_delivery_date from public.s3_swiggy_purchase_orders s LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, swiggy_sku as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY swiggy_sku ORDER BY LENGTH(COALESCE(swiggy_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON LOWER(REPLACE(REPLACE(s.skucode::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ;",
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
            