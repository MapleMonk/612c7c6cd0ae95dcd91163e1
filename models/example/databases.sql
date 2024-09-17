{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table pomme_db.maplemonk.ongoing_fact_items as select a.*, pa.color_general, pa.size, pa.gender, pa.product_brand, pa.color_specific, pa.attribute_1, pa.attribute_2, pa.attribute_3, pa.supplier, pa.model, pa.type, c.productname, c.category1, c.category2, case when lower(productname) like \'%bling%\' then \'Bling\' when lower(productname) like \'%classic%\' then \'Classic\' end as Print, c.grip from (select left(orderinfo:createdDate::string,10)::date order_Date ,A.value:article:articleNumber::string SKU , A.value:orderedNumberOfItems::string quantity_sold from pomme_db.maplemonk.ongoing_orders, lateral flatten (input => orderlines) A ) a left join pomme_db.maplemonk.product_attributes_amiko pa on pa.sku = a.sku left join (select sku, productname, category1, category2, grip from (select distinct sku, productname, category1, category2, grip, row_number() over (partition by sku order by 1) rw from pomme_db.maplemonk.SALES_CONSOLIDATED_pomme where sku is not null ) where rw=1 ) c on c.sku = a.sku ;",
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
            