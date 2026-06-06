{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table cycle_count_report as with source as ( select * from get_cycle_count_report ), renamed as ( select code::varchar as cc_code, type::varchar as cc_type, try_to_number(excess)::integer as excess, \"CC Scope\"::varchar as cc_scope, \"SKU Code\"::varchar as sku_code, \"CC Status\"::varchar as cc_status, \"CW Status\"::varchar as cw_status, try_to_number(\"Not Found\")::integer as not_found, \"Batch Code\"::varchar as batch_code, \"CC Context\"::varchar as cc_context, \"Count Wave\"::varchar as count_wave, \"Shelf Code\"::varchar as shelf_code, try_to_number(\"Approver ID\")::integer as approver_id, \"Shelf Status\"::varchar as shelf_status, try_to_timestamp_ntz(\"Approval Time\") as approval_timestamp, try_to_date(\"CC Target Date\") as cc_target_date, try_to_date(\"CW Target Date\") as cw_target_date, try_to_number(\"Count Variance\")::integer as count_variance, \"Inventory Type\"::varchar as inventory_type, try_to_number(\"Previous Count\")::integer as previous_count, try_to_timestamp_ntz(\"CC Created Date\") as cc_created_at, try_to_timestamp_ntz(\"CC Updated Date\") as cc_updated_at, try_to_number(\"Counted Quantity\")::integer as counted_qty, try_to_number(\"Expected Quantity\")::integer as expected_qty, try_to_timestamp_ntz(\"CW Created (datetime)\") as cw_created_at, \"Count Operator (email)\"::varchar as count_operator_email from source ) select * from renamed;",
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
            