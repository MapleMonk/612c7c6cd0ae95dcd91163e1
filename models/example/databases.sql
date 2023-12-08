{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.retailer_lead as select lrl.id as lead_id, cast(timestampadd(minute, 660, lrl.created_at) as date)created_at, cast(timestampadd(minute, 660, lrl.modified_at) as date) modified_at, cast(timestampadd(minute, 660, date) as date) date, lrl.store_name, lrl.billing_name, lrl.email, lrl.address, lrl.landmark, lrl.market_name, lrl.society_name, lrl.latitude, lrl.longitude, lrl.sq_feet_input, lrl.ac_availability, lrl.perishable_items_availability, lrl.counter_type, lrl.refrigerator_type, lrl.soh, lrl.brand_premiums, lrl.store_accessible, lrl.revenue_per_day, lrl.program_type, lrl.store_type, lrl.society_type, lrl.final_score, lrl.classification, lrl.channel_classification, lrl.status as lead_status, lrl.is_validated, lrl.remark, lrl.milk_booth, lrl.meat_store, lrl.vegetable_store, lrl.city_id, rr.code, bc.city_name, lrl.cluster_id, lrl.lead_soh_id, lrl.retailer_id,rr.onboarding_status as retailer_onboarding_status,rr.salesPersonProfile_id as retailer_salesPersonProfile_id,cu.name as sales_person, lrl.sales_person_profile_id as lead_salesPersonProfile_id, lrl.zone_id, bcl.cluster_name , llf.from_status,llf.to_status,llf.remark as leadfollowup_remark,llf.created_by_id as leadfollowup_person,llf.retailer_lead_id from eggozdb.maplemonk.my_sql_lead_retailerlead lrl left join eggozdb.maplemonk.my_sql_lead_leadfollowup llf on lrl.id = llf.retailer_lead_id left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = lrl.city_id left join eggozdb.maplemonk.my_sql_base_cluster bcl on bcl.id = lrl.cluster_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = lrl.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on ssp.user_id=cu.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on lrl.retailer_id=rr.id ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        