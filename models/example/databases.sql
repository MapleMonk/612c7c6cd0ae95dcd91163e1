{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ghc_db.maplemonk.klaviyo_profiles_fact_items as select attributes:email::string email ,attributes:first_name::string first_name ,attributes:last_name::string last_name ,attributes:location:address1::string address1 ,attributes:location:address2::string address2 ,attributes:location:zip::string zip ,attributes:location:timezone::string timezone ,attributes:location:region::string region ,attributes:properties:\"Accepts Marketing\"::string accepts_marketing ,attributes:subscriptions:email:marketing:consent::String email_marketing_consent ,attributes:subscriptions:sms:marketing:consent::String sms_marketing_consent from ghc_db.maplemonk.klaviyo_profiles ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from ghc_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            