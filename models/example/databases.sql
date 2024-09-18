{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table MapleMonk.zouk_MARKETING_CONSOLIDATED_DTC as select * from `MapleMonk.zouk_MARKETING_CONSOLIDATED` where lower(Channel) like any (\'%facebook%\',\'%google%\',\'%contlo%\',\'%bitespeed%\') ; Create or replace table MapleMonk.zouk_MARKETING_CONSOLIDATED_MP as select * from `MapleMonk.zouk_MARKETING_CONSOLIDATED` where not(lower(Channel) like any (\'%facebook%\',\'%google%\',\'%contlo%\',\'%bitespeed%\')) ; Create or replace table maplemonk.zouk_sales_consolidated_MP as select * from maplemonk.zouk_sales_consolidated where lower(MARKETPLACE_SEGMENT) like \'%marketplace%\' ; Create or replace table maplemonk.zouk_sales_consolidated_DTC as select * from maplemonk.zouk_sales_consolidated where not(lower(MARKETPLACE_SEGMENT) like any (\'%marketplace%\',\'%offline%\')) ; Create or replace table maplemonk.zouk_sales_cost_source_DTC as select * from maplemonk.zouk_sales_cost_source where lower(Marketplace) like any (\'%shopify%\',\'%website%\',\'app\') ; Create or replace table maplemonk.zouk_sales_cost_source_MP as select * from maplemonk.zouk_sales_cost_source where not(lower(Marketplace) like any (\'%shopify%\',\'%website%\',\'app\',\'%offline%\')) ;",
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
            