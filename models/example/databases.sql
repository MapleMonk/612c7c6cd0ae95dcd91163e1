{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.snitch.shopify_customer_segmentation as select updated.*, try_cast(updated.pincode as int) as pincode1, concat(\'d\',cast(div0(discount_percentage,10)::int as varchar)) discount_floor, pn_map.STATENAME as STATE, pn_map.ISO_3166_2_CODE as STATE_CODE, pn_map.district as district from ( select cmv_s.*, cd.first_name, cd.last_name, cd.name, cd.pincode, cd.email, right(regexp_replace(cd.phone, \'[^a-zA-Z0-9]+\'),10) as number, case when number is not null then concat(\'91\',number) end as phone from snitch_db.snitch.customer_master_view_shopify cmv_s left join (select * from(select pincode,email,customer_id_final,phone,first_name,last_name,name,row_number() over(partition by customer_id_final order by email ASC)rw from snitch_db.snitch.customer_details_dim where lower(source_channel) = \'shopify\' )where rw=1) cd on cmv_s.customer_id_final = cd.customer_id_final )updated left join ( select pin.pincode, pin.district, pin.statename, iso.ISO_3166_2_CODE from (select * from (select pincode , STATENAME, district , row_number() over(partition by pincode order by STATENAME)rw from snitch_db.maplemonk.pincode_mapping )where rw=1)pin left join (select * from ( select ISO_3166_2_CODE, SUBDIVISION_NAME, row_number() over(partition by ISO_3166_2_CODE order by 1 ) rw from snitch_db.maplemonk.iso_state_mapping ) where rw=1)iso on lower(iso.SUBDIVISION_NAME) = lower(pin.statename) ) as pn_map on pn_map.pincode = updated.pincode",
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
                        