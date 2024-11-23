{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table maplemonk.zouk_db_GVS_Sales as WITH Targets as ( select cast(date as date) as date, MARKETPLACE, sum(ifnull(cast(replace(target,\',\',\'\') as int64),0)) target, sum(ifnull(cast(replace(Offline_Spends,\',\',\'\') as int64),0)) Offline_Spends, from `MapleMonk.zouk_db_GVS_Target` group by 1,2 ), Marketplace_Spends as ( select date ,case when lower(channel) like any (\'%facebook%\', \'%google%\') then \'SHOPIFY\' when lower(channel) like any (\'%amazon%\') then \'AMAZON\' when lower(channel) like \'%flipkart%\' then \'FLIPKART\' when lower(channel) like \'%myntra%\' then \'MYNTRA\' else upper(channel) end as Marketplace ,sum(ifnull(spend,0)) as spend from maplemonk.zouk_MARKETING_CONSOLIDATED group by 1,2 ), Orders as ( select cast(FI.order_date as date) Date ,upper(marketplace) Marketplace ,ifnull(sum(ifnull(FI.SELLING_PRICE,0)),0) Total_Sales from maplemonk.zouk_Secondary_SALES_CONSOLIDATED FI group by 1,2 ) select upper(coalesce(t.Marketplace,o.marketplace,m.Marketplace)) Marketplace ,coalesce(t.date,o.date,m.date) as date ,o.Total_Sales ,t.Offline_Spends ,t.target ,m.spend from Orders o full outer join Targets t on o.date = t.date and trim(lower(o.Marketplace)) = trim(lower(t.Marketplace)) full outer join Marketplace_Spends M on m.date = coalesce(t.date,o.date) and trim(lower(m.Marketplace)) = trim(lower(coalesce(t.Marketplace,o.marketplace)))",
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
            