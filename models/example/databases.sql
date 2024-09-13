{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table pomme_db.maplemonk.mintsoft_fact_items as select a.*, pa.color, pa.brand, pa.gender, pa.pattern, pa.size, c.productname, c.category1, c.category2, case when left(productname, position(\' \',productname,1)-1) in (\'Club\',\'Saddle\',\'Socks\') then \'Club\' else replace(left(productname, position(\' \',productname,1)-1),\',\',\'\') end as model, case when lower(productname) like \'%bling%\' then \'Bling\' when lower(productname) like \'%classic%\' then \'Classic\' end as Print, c.grip from (select ORDERDATE::date order_Date ,A.value:SKU::string SKU , A.value:Quantity::string quantity_sold from pomme_db.maplemonk.mintsoft_pomme_prod_orders, lateral flatten (input => orderitems) A ) a left join pomme_db.maplemonk.product_attributes pa on pa.sku = a.sku left join (select sku, productname, category1, category2, grip from (select distinct sku, productname, category1, category2, grip, row_number() over (partition by sku order by 1) rw from pomme_db.maplemonk.SALES_CONSOLIDATED_pomme where sku is not null ) where rw=1 ) c on c.sku = a.sku ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from Pomme_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            