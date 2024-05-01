{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table rpsg_db.maplemonk.drv_marketing_product_spends as select date, ad_name, channel, product, category, sum(spend) spend from (select distinct date, ad_name, channel, ifnull(b.product,\'Others\') product, ifnull(b.category,\'Others\') category, spend from RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV a left join (select * from (select *, row_number() over (partition by \"Ad Name Contains\" order by 1) rw from rpsg_db.maplemonk.mapping_ad_product_category) where rw = 1 ) b on lower(coalesce(a.ad_name,adset_name)) like \'%\' ||lower(b.\"Ad Name Contains\")|| \'%\' where (account_name = \'DRV Ad Account\' or account_name is null) and date >= \'2023-04-01\' ) group by 1,2,3,4,5 ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from RPSG_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        