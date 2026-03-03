{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.saadaa_po_otiff_report as with po_data as ( select po_ref_num, vendor_name, vendor_code, po_created_date, expected_delivery_date, po_original_quantity, grn_receive_quantity, po_status, last_grn_date, case when date(expected_delivery_date) is null then cast(null as string) when date(last_grn_date) <= date(expected_delivery_date) then \'Yes\' else \'No\' end as on_time_delivery, po_type, case when po_type is null or po_type = \'\' then null when po_type in (\'JOB\', \'FOB\', \'EFOB\') and grn_receive_quantity >= (0.99 * po_original_quantity) then \'Yes\' else \'No\' end as in_fill, grn_created_date as first_grn_date from maplemonk.saadaa_po_grn_mapping ) select *, case when lower(on_time_delivery) = \'yes\' and lower(in_fill) = \'yes\' then \'Yes\' when on_time_delivery is null or in_fill is null then null else \'No\' end as otiff, case when lower(on_time_delivery) like \'no\' then date_diff(date(last_grn_date),date(expected_delivery_date),day) end as delay_days, cast(c.total_cutting_qty as int64) as cutting_register_qty, case when po_type is null or po_type = \'\' then null when po_type in (\'JOB\', \'FOB\', \'EFOB\') and grn_receive_quantity >= (0.99 * cast(c.total_cutting_qty as int64)) then \'Yes\' else \'No\' end as in_fill_cutting_register, case when on_time_delivery is null or po_type is null or po_type = \'\' then null when on_time_delivery = \'Yes\' and (po_type in (\'JOB\', \'FOB\', \'EFOB\') and grn_receive_quantity >= (0.99 * cast(c.total_cutting_qty as int64))) then \'Yes\' else \'No\' end as otiff_cutting_register from po_data p left join maplemonk.otiff_sheet8 c on trim(upper(p.po_ref_num)) = trim(upper(c.po_number)) ;",
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
            