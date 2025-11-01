{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table Franchise_Model as SELECT Date, retailer_name, area_classification, sale_type, SKU, PRODUCT_TYPE, RETAILER_TYPE,DISTRIBUTOR,null as DEALER_NAME,REVENUE, eggs_sold from primary_and_secondary_sku union all select DELIVERY_DATE as Date, code as retailer_name, area_classification,case when lower(retailer_category) =\'tertiary\' then \'tertiary\' end as sale_type, sku ,product_type ,retailer_category as Retailer_type,distributor_name as DISTRIBUTOR,DEALER_NAME ,sku_sale as Revenue, eggs_sold from tertiary_sales ;",
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
            