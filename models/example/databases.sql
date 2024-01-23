{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create table snitch_db.maplemonk.ecoreturns_master as with order_data as ( select marketplace_mapped, order_name, order_date, sum(selling_price) as gross_without_tax, sum(tax) as total_tax, sum(mrp) as mrp, sum(cost) as total_cost, count(distinct AWB) as number_of_shipments, count(sku) as number_of_items, sum(return_flag) as number_of_items_returned from snitch_db.maplemonk.unicommerce_availability_merge where marketplace_mapped in (\'SHOPIFY\') group by order_name,marketplace_mapped,order_date order by order_name ), ecoret as ( select * from snitch_db.maplemonk.ecoreturns_data ) select * from ecoret left join order_data on ecoret.order_id=order_data.order_name",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        