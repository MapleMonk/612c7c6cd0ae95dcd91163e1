{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS PUBLIC.anveshan_blinkit_purchase_orders_fact_items; CREATE TABLE PUBLIC.anveshan_blinkit_purchase_orders_fact_items AS Select \'BLINKIT\' as Channel, mrp::DOUBLE PRECISION as mrp, upc::varchar as upc, name::varchar as product_name, item_id::varchar as product_id, upper(po_state::varchar) as PO_Status, po_number::varchar as po_number, (tax_value::double precision)/100 as tax_percent, cess_value::double precision as cess_value, (cgst_value::double precision)/100 as cgst_percent, (igst_value::double precision)/100 as igst_percent, (sgst_value::double precision)/100 as sgst_percent, cost_price::double precision as cost_price, left(order_date,10)::date as order_date, left(expiry_date,10)::date as expiry_date, vendor_name::varchar as vendor_name, landing_rate::double precision as landing_rate, total_amount::double precision as total_amount, facility_name::varchar as facility_name, units_ordered::bigint as units_ordered, left(appointment_date,10)::date as appointment_date, remaining_quantity::bigint as remaining_quantity, replacE(entity_vendor_legal_name,\'\"\',\'\') as entity_name, upper(SPLIT_PART(TRIM(REPLACE(facility_name, \'Super Store\', \'\')), \' \', 1)) as City from public.blinkit_anveshan_po_summary_partner_biz b",
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
            