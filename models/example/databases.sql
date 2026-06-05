{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.webengage_marketing_cost as with data as ( select cast(day as date) date, created_by, channel, campaign_id, campaign_name, Type_of_Campaign, journey_id, journey_name, Variation_ID, variation_name, status, sum(cast(sent as float64)) sent, sum(cast(failed as float64)) failed, sum(cast(delivered as float64)) delivered, sum(cast(Revenue__INR_ as float64)) revenue, sum(cast(unique_clicks as float64)) unique_clicks from `MapleMonk.Rosier_webengage_gs_csv_from_mail` group by 1,2,3,4,5,6,7,8,9,10,11 ) select data.*, case when channel = \'WhatsApp\' then delivered * 0.88 when channel = \'Email\' then sent*0.2 when channel = \'RCS\' then sent*0.18 when channel = \'SMS\' then sent*0.12 end as cost from data ;",
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
            