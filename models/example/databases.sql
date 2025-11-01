{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table Franchise_Model as SELECT Date,null as first_month, retailer_name, area_classification, sale_type, SKU, PRODUCT_TYPE, RETAILER_TYPE,DISTRIBUTOR,null as DEALER_NAME,REVENUE, eggs_sold from primary_and_secondary_sku union all select DELIVERY_DATE as Date,null as first_month, code as retailer_name, area_classification,case when lower(retailer_category) =\'tertiary\' then \'tertiary\' end as sale_type,sku ,product_type ,retailer_category as Retailer_type,distributor_name as DISTRIBUTOR,DEALER_NAME ,sku_sale as Revenue, eggs_sold from tertiary_sales union all select crm_date as Date, first_month, code as retailer_name,null as area_classification,case when lower(code) is not null then \'Franchise\' end as sale_type , crm_sku as SKU, null as product_type, null as Retailer_type, null as DISTRIBUTOR, null as DEALER_NAME, crm_sale as Revenue, crm_eggs_sold as eggs_sold from crm_overview;",
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
            