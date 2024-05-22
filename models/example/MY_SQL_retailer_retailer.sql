{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.my_sql_tertiary_retailer_retailer as select rr.*,r1.code as dealer, r2.code as distributor from eggozdb.maplemonk.my_sql_tertiary_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_retailer r1 on rr.dealer_id = r1.id left join eggozdb.maplemonk.my_sql_retailer_retailer r2 on r1.distributor_id = r2.id; left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_customer_subcategory rcs on rcs.id = rr.sub_category_id where rr.dealer_id is not null ;",
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
                        