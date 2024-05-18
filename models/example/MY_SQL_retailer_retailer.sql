{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.my_sql_tertiary_retailer_retailer as select rr.*, rcc.name as retailer_category, rcs.name as retailer_subcategory from eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_customer_subcategory rcs on rcs.id = rr.sub_category_id where rr.dealer_id is not null ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.MY_SQL_retailer_retailer
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        